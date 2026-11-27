from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.parse import urlsplit

import yaml
from sqlalchemy.engine import URL, make_url


class Rejected(ValueError):
    pass


DRIVERS = {"mysql": "mysql+pymysql", "mariadb": "mariadb+pymysql",
           "postgresql": "postgresql+psycopg", "mssql": "mssql+pyodbc",
           "oracle": "oracle+oracledb", "sqlite": "sqlite"}
URL_OPTIONS = {
    "mysql": {"charset", "ssl_ca", "ssl_cert", "ssl_key", "ssl_verify_cert", "ssl_verify_identity"},
    "mariadb": {"charset", "ssl_ca", "ssl_cert", "ssl_key", "ssl_verify_cert", "ssl_verify_identity"},
    "postgresql": {"sslmode", "sslrootcert", "sslcert", "sslkey", "application_name"},
    "mssql": {"driver", "encrypt", "trustservercertificate"},
    "oracle": {"service_name"}, "sqlite": set(),
}


def validate_url_options(url: URL):
    dialect = target_from_url(url.render_as_string(hide_password=True)).dialect
    if any(key.lower() not in URL_OPTIONS[dialect] for key in url.query):
        raise Rejected("Unsafe or unsupported driver URL option; endpoint/credentials/init SQL overrides are forbidden")
    return url


@dataclass(frozen=True)
class Target:
    dialect: str
    host: str | None = None
    port: int | None = None
    database: str | None = None


def target_from_url(value: str) -> Target:
    if "${" in value:
        raise Rejected("Unresolved datasource placeholder; specify a connection or resolved target")
    value = value.removeprefix("jdbc:")
    if value.startswith("sqlserver://"):
        host, *options = value[len("sqlserver://"):].split(";")
        parsed = urlsplit("mssql://" + host)
        database = next((x.split("=", 1)[1] for x in options if x.lower().startswith("databasename=")), None)
        return Target("mssql", parsed.hostname, parsed.port or 1433, database)
    if value.startswith("oracle:thin:@"):
        value = value[len("oracle:thin:@"):]
        match = re.fullmatch(r"(?://)?([^:/]+):(\d+)(?:/|:)([^?]+)", value)
        if not match:
            raise Rejected("Unsupported Oracle datasource URL; specify connection explicitly")
        return Target("oracle", match[1].lower(), int(match[2]), match[3])
    if value.startswith("sqlite:") and not value.startswith("sqlite://"):
        return Target("sqlite", database=str(Path(value[len("sqlite:"):]).resolve()))
    try:
        parsed = make_url(value)
        dialect = {"postgres": "postgresql", "sqlserver": "mssql"}.get(parsed.get_backend_name(), parsed.get_backend_name())
        if dialect not in DRIVERS:
            raise Rejected("Unsupported database type")
        database = parsed.database
        if dialect == "oracle":
            database = parsed.query.get("service_name", database)
        if dialect == "sqlite" and database:
            database = str(Path(database).resolve())
        defaults = {"mysql": 3306, "mariadb": 3306, "postgresql": 5432, "mssql": 1433, "oracle": 1521}
        return Target(dialect, (parsed.host or "").lower() or None, parsed.port or defaults.get(dialect), database)
    except Rejected:
        raise
    except Exception:
        raise Rejected("Invalid datasource URL") from None


@dataclass(frozen=True)
class Connection:
    name: str
    url: URL
    aliases: tuple[str, ...] = ()
    allowed_databases: tuple[str, ...] = ()
    max_rows: int = 1000
    timeout_seconds: int = 15

    @property
    def target(self) -> Target:
        return target_from_url(self.url.render_as_string(hide_password=True))

    def public(self):
        return {"name": self.name, "dialect": self.target.dialect, "host": self.target.host,
                "port": self.target.port, "database": self.target.database,
                "aliases": list(self.aliases), "maxRows": self.max_rows,
                "timeoutSeconds": self.timeout_seconds}


def load_connections(path: Path | None = None) -> list[Connection]:
    entries = []
    if path and path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        if set(data) != {"connections"}:
            raise Rejected("Connection file must contain only connections")
        for item in data["connections"]:
            allowed = {"name", "url", "url_env", "username_env", "password_env", "aliases", "allowed_databases", "max_rows", "timeout_seconds"}
            if set(item) - allowed:
                raise Rejected("Unknown connection configuration key")
            raw = item.get("url") or os.environ.get(item.get("url_env", ""))
            if not raw:
                raise Rejected("Connection URL is missing")
            url = make_url(raw)
            # Configuration URLs may never contain inline credentials.
            if item.get("url") and (url.username or url.password):
                raise Rejected("Inline credentials forbidden; use environment references")
            if item.get("username_env"):
                username = os.environ.get(item["username_env"])
                if not username:
                    raise Rejected("Dedicated readonly username environment variable is missing")
                url = url.set(username=username)
            if item.get("password_env"):
                password = os.environ.get(item["password_env"])
                if password is None:
                    raise Rejected("Dedicated readonly password environment variable is missing")
                url = url.set(password=password)
            dialect = target_from_url(url.render_as_string()).dialect
            url = url.set(drivername=DRIVERS[dialect])
            entries.append(Connection(item["name"], url, tuple(item.get("aliases", [])), tuple(item.get("allowed_databases", [])), int(item.get("max_rows", 1000)), int(item.get("timeout_seconds", 15))))
    if not entries and os.environ.get("MYSQL_USER"):
        entries.append(Connection("mysql-local", URL.create("mysql+pymysql", username=os.environ["MYSQL_USER"], password=os.environ.get("MYSQL_PASS", ""), host=os.environ.get("MYSQL_HOST", "localhost"), port=int(os.environ.get("MYSQL_PORT", "3306")), database=os.environ.get("MYSQL_DB") or None), ("database_readonly",)))
    if len({e.name for e in entries}) != len(entries):
        raise Rejected("Duplicate connection name")
    for entry in entries:
        validate_url_options(entry.url)
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", entry.name) or not 1 <= entry.max_rows <= 10000 or not 1 <= entry.timeout_seconds <= 60:
            raise Rejected("Invalid connection name or limits")
        if entry.target.dialect == "sqlite" and (not entry.target.database or entry.target.database == ":memory:"):
            raise Rejected("SQLite requires an existing file")
    for key, value in os.environ.items():
        if (key.startswith("ALLOW_") and key.endswith("_OPERATION") or key in {"MULTI_DB_WRITE_MODE", "MYSQL_DISABLE_READ_ONLY_TRANSACTIONS"}) and value.lower() == "true":
            raise Rejected("Write-enabling configuration is forbidden")
    return entries


def project_targets(project: str, profile: str | None = None) -> list[Target]:
    root = Path(project).resolve()
    folders = [root, root / "src/main/resources", root / "config"]
    bases = [f / ("application" + ext) for f in folders for ext in (".yml", ".yaml", ".properties") if (f / ("application" + ext)).is_file()]
    if not bases:
        # Handle a standard Maven multi-module project without scanning arbitrary files.
        bases = sorted(root.glob("*/src/main/resources/application.yml")) + sorted(root.glob("*/src/main/resources/application.yaml")) + sorted(root.glob("*/src/main/resources/application.properties"))
    def read(file):
        if file.suffix == ".properties":
            documents, result = [], {}
            for line in file.read_text(encoding="utf-8-sig").splitlines():
                if line.strip() in {"#---", "!---"}:
                    documents.append(result)
                    result = {}
                    continue
                match = re.match(r"\s*([^#!\s][^=]*?)\s*[=:]\s*(.*)$", line)
                if match and (match[1].endswith((".url", ".jdbc-url")) or match[1].startswith(("spring.profiles.", "spring.config.import")) or match[1] in {"spring.config.activate.on-profile", "spring.profiles"}):
                    if (len(match[2]) - len(match[2].rstrip("\\"))) % 2:
                        raise Rejected("Continued properties values require an explicit resolved target")
                    result[match[1]] = match[2].strip()
            return documents + [result]
        return [x for x in yaml.safe_load_all(file.read_text(encoding="utf-8-sig")) if isinstance(x, dict)]
    def flatten(value, prefix=""):
        result = {}
        for key, item in value.items():
            name = prefix + str(key)
            if isinstance(item, dict):
                result.update(flatten(item, name + "."))
            else:
                result[name] = item
        return result
    targets = []
    for base in bases:
        docs = [flatten(d) for d in read(base)]
        detected = {str(d.get("spring.profiles.active", d.get("spring.profiles.default"))) for d in docs if d.get("spring.profiles.active", d.get("spring.profiles.default"))}
        if not profile and len(detected) > 1:
            raise Rejected("Multiple active profiles; specify profile")
        active = profile or next(iter(detected), 'default')
        if active and not re.fullmatch(r"[A-Za-z0-9_.-]+", active):
            raise Rejected("Composite or unresolved profile; specify one profile")
        effective = {}
        for doc in docs:
            activated = doc.get("spring.config.activate.on-profile", doc.get("spring.profiles"))
            if activated is not None and not re.fullmatch(r"[A-Za-z0-9_.-]+", str(activated)):
                raise Rejected("Profile expressions require an explicit resolved target")
            if activated is None or str(activated) == active:
                effective.update(doc)
        if active:
            for ext in (".yml", ".yaml", ".properties"):
                file = base.parent / ("application-" + active + ext)
                if file.is_file():
                    for doc in read(file):
                        flattened = flatten(doc)
                        activated = flattened.get("spring.config.activate.on-profile", flattened.get("spring.profiles"))
                        if activated is not None and not re.fullmatch(r"[A-Za-z0-9_.-]+", str(activated)):
                            raise Rejected("Profile expressions require an explicit resolved target")
                        if activated is None or str(activated) == active:
                            effective.update(flattened)
        if any(k.startswith("spring.config.import") or k.startswith("spring.profiles.include") or k.startswith("spring.profiles.group") for k in effective):
            raise Rejected("External config/profile composition needs an explicit resolved target")
        # RuoYi/Druid master is the actual primary datasource, overriding the generic URL.
        if "spring.datasource.druid.master.url" in effective:
            effective.pop("spring.datasource.url", None)
        for key, value in effective.items():
            if "datasource" in key and key.endswith((".url", ".jdbc-url")) and isinstance(value, str):
                targets.append(target_from_url(value))
    unique = list(dict.fromkeys(targets))
    if not unique:
        raise Rejected("No datasource URL found; supply connection or datasource_url")
    return unique


def resolve(entries: list[Connection], *, connection=None, datasource_url=None, project=None, profile=None, database=None, dialect=None) -> Connection:
    targets = project_targets(project, profile) if project else []
    if datasource_url:
        explicit = target_from_url(datasource_url)
        if targets and explicit not in targets:
            raise Rejected("Datasource conflicts with project configuration")
        targets = [explicit]
    if len(targets) > 1:
        raise Rejected("Multiple datasource targets; specify datasource_url from the project")
    target = targets[0] if targets else None
    if dialect and target and dialect != target.dialect:
        raise Rejected("Dialect conflicts with datasource target")
    if database and target and database != target.database:
        raise Rejected("Database conflicts with datasource target")
    database = database or (target.database if target else None)
    candidates = []
    for entry in entries:
        current = entry.target
        if entry.allowed_databases and current.database and current.database not in entry.allowed_databases:
            continue
        if database and entry.allowed_databases and database not in entry.allowed_databases:
            continue
        if connection and connection not in (entry.name, *entry.aliases):
            continue
        if dialect and current.dialect != dialect:
            continue
        if target and (current.dialect != target.dialect or current.host != target.host or current.port != target.port):
            continue
        if database and current.database != database:
            if current.dialect not in {"mysql", "mariadb"} or current.database is not None:
                continue
            if entry.allowed_databases and database not in entry.allowed_databases:
                continue
        candidates.append(entry)
    if len(candidates) != 1:
        raise Rejected("Connection match is ambiguous or absent; candidates=" + ",".join(e.name for e in candidates))
    selected = candidates[0]
    if database and selected.target.dialect in {"mysql", "mariadb"}:
        selected = replace(selected, url=selected.url.set(database=database))
    return selected
