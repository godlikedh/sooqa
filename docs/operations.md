# Operations

The development Compose file provides PostgreSQL. Production deployment,
database backup, Telegram configuration, and media tool requirements will be
documented as those runtime components are added.

## Local container runtime

Docker Desktop is optional on macOS. Colima supplies the Docker-compatible
engine without a Docker account:

    brew install colima
    colima start --runtime docker
    docker context use colima

Verify the selected engine with `docker context show`, then use the normal
Compose commands. Stop the VM with `colima stop` when it is not needed.

The B1 migration command is:

    DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa cargo run -p sooqa-server -- migrate
