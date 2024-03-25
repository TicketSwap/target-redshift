"""Redshift target class."""

from __future__ import annotations

from pathlib import PurePath

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_redshift.sinks import RedshiftSink


class TargetRedshift(SQLTarget):
    """Target for Redshift."""

    _MAX_RECORD_AGE_IN_MINUTES: float = 60

    def __init__(
        self,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Initialize the target.

        Args:
            config: Target configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        self.max_parallelism = 1
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        # There's a few ways to do this in JSON Schema but it is schema draft dependent.
        # https://stackoverflow.com/questions/38717933/jsonschema-attribute-conditionally-required # noqa: E501
        assert (self.config.get("sqlalchemy_url") is not None) or (
            self.config.get("host") is not None
            and self.config.get("port") is not None
            and self.config.get("user") is not None
            and self.config.get("password") is not None
            and self.config.get("dialect+driver") is not None
        ), ("Need either the sqlalchemy_url to be set or host, port, user," + "password, and dialect+driver to be set")

        # If sqlalchemy_url is not being used and ssl_enable is on, ssl_mode must have
        # one of six allowable values. If ssl_mode is verify-ca or verify-full, a
        # certificate authority must be provided to verify against.
        assert (
            (self.config.get("sqlalchemy_url") is not None)
            or (self.config.get("ssl_enable") is False)
            or (self.config.get("ssl_mode") in {"disable", "allow", "prefer", "require"})
        )

        assert self.config.get("add_record_metadata") or not self.config.get("activate_version"), (
            "Activate version messages can't be processed unless add_record_metadata "
            "is set to true. To ignore Activate version messages instead, Set the "
            "`activate_version` configuration to False."
        )

    name = "target-redshift"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description=("Hostname for redshift instance. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=5432,
            description=(
                "The port on which redshift is awaiting connection. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "user",
            th.StringType,
            description=("User name used to authenticate. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "password",
            th.StringType,
            description=("Password used to authenticate. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "database",
            th.StringType,
            description=("Database name. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description=(
                "SQLAlchemy connection string. "
                + "This will override using host, user, password, port, "
                + "dialect, and all ssl settings. Note that you must escape password "
                + "special characters properly. See "
                + "https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords"
            ),
        ),
        th.Property(
            "dialect+driver",
            th.StringType,
            default="redshift+redshift_connector",
            description=(
                "Dialect+driver see "
                + "https://aws.amazon.com/blogs/big-data/use-the-amazon-redshift-sqlalchemy-dialect-to-interact-with-amazon-redshift. "
                + "Generally just leave this alone. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "aws_copy_role_arn",
            th.StringType,
            secret=True,  # Flag config as protected.
            required=True,
            description="Redshift copy role arn to use for the COPY command from s3",
        ),
        th.Property(
            "s3_bucket",
            th.StringType,
            required=True,
            description="S3 bucket to save staging files before using COPY command",
        ),
        th.Property(
            "s3_path",
            th.StringType,
            description="S3 path to save staging files before using COPY command",
        ),
        th.Property(
            "remove_s3_files",
            th.BooleanType,
            default=False,
            description="If you want to remove staging files in S3",
        ),
        th.Property(
            "temp_file_path", th.StringType, default="temp", description="Where you want to store your temp data files."
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="Redsdhift schema to send data to, example: tap-clickup",
        ),
        th.Property(
            "activate_version",
            th.BooleanType,
            default=False,
            description=(
                "If set to false, the tap will ignore activate version messages. If "
                + "set to true, add_record_metadata must be set to true as well."
            ),
        ),
        th.Property(
            "hard_delete",
            th.BooleanType,
            default=False,
            description=(
                "When activate version is sent from a tap this specefies "
                + "if we should delete the records that don't match, or mark "
                + "them with a date in the `_sdc_deleted_at` column. This config "
                + "option is ignored if `activate_version` is set to false."
            ),
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            default=False,
            description=(
                "Note that this must be enabled for activate_version to work!"
                + "This adds _sdc_extracted_at, _sdc_batched_at, and more to every "
                + "table. See https://sdk.meltano.com/en/latest/implementation/record_metadata.html "  # noqa: E501
                + "for more information."
            ),
        ),
        th.Property(
            "ssl_enable",
            th.BooleanType,
            default=False,
            description=(
                "Whether or not to use ssl to verify the server's identity. Use"
                + " ssl_certificate_authority and ssl_mode for further customization."
                + " To use a client certificate to authenticate yourself to the server,"
                + " use ssl_client_certificate_enable instead."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_mode",
            th.StringType,
            default="verify-full",
            description=(
                "SSL Protection method, see [postgres documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION)"
                + " for more information. Must be one of disable, allow, prefer,"
                + " require, verify-ca, or verify-full."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
    ).to_dict()

    default_sink_class = RedshiftSink


if __name__ == "__main__":
    TargetRedshift.cli()
