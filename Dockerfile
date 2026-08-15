FROM rust:1.97-bookworm AS builder

WORKDIR /usr/src/sooqa
COPY . .
RUN cargo build --release --workspace

FROM debian:bookworm-slim

ARG YTDLP_VERSION=2026.06.09
ARG YTDLP_AMD64_SHA256=bf8aac79b72287a6d2043074415132558b43743a8f9461a22b0141e90f16ce66
ARG YTDLP_ARM64_SHA256=cabd246445bdfde0eda0dfe68bbe90354be83f3fdbbf077df11a2ea55f41cdbd
ARG DENO_VERSION=2.8.1
ARG DENO_AMD64_SHA256=2d7bb6195226ac832e0bf7109a115f0af65ee69ac797a4bbde5b27a06cc242d9
ARG DENO_ARM64_SHA256=67e9df91870fd0af700df924173e3009ea7ff6956e2c3c3bb86065d6070d0fd6
ARG BGUTIL_VERSION=1.3.1
ARG BGUTIL_PLUGIN_SHA256=b8ceec7f76143da172aaf5ebeec0c2d218e5680c063b931586bca48567069b38

RUN groupadd --system sooqa && useradd --system --gid sooqa sooqa \
    && apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates curl ffmpeg unzip \
    && case "$(dpkg --print-architecture)" in \
        amd64) \
            ytdlp_asset=yt-dlp_linux; ytdlp_sha256="$YTDLP_AMD64_SHA256"; \
            deno_asset=deno-x86_64-unknown-linux-gnu.zip; deno_sha256="$DENO_AMD64_SHA256" ;; \
        arm64) \
            ytdlp_asset=yt-dlp_linux_aarch64; ytdlp_sha256="$YTDLP_ARM64_SHA256"; \
            deno_asset=deno-aarch64-unknown-linux-gnu.zip; deno_sha256="$DENO_ARM64_SHA256" ;; \
        *) echo "unsupported Debian architecture: $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac \
    && curl --fail --silent --show-error --location --retry 3 \
        "https://github.com/yt-dlp/yt-dlp/releases/download/${YTDLP_VERSION}/${ytdlp_asset}" \
        --output /tmp/yt-dlp \
    && echo "${ytdlp_sha256}  /tmp/yt-dlp" | sha256sum --check --status - \
    && install --mode=0755 /tmp/yt-dlp /usr/local/bin/yt-dlp \
    && curl --fail --silent --show-error --location --retry 3 \
        "https://github.com/denoland/deno/releases/download/v${DENO_VERSION}/${deno_asset}" \
        --output /tmp/deno.zip \
    && echo "${deno_sha256}  /tmp/deno.zip" | sha256sum --check --status - \
    && unzip -p /tmp/deno.zip deno > /usr/local/bin/deno \
    && chmod 0755 /usr/local/bin/deno \
    && mkdir -p /usr/local/share/sooqa/yt-dlp-plugins \
    && curl --fail --silent --show-error --location --retry 3 \
        "https://github.com/Brainicism/bgutil-ytdlp-pot-provider/releases/download/${BGUTIL_VERSION}/bgutil-ytdlp-pot-provider.zip" \
        --output /tmp/bgutil-ytdlp-pot-provider.zip \
    && echo "${BGUTIL_PLUGIN_SHA256}  /tmp/bgutil-ytdlp-pot-provider.zip" | sha256sum --check --status - \
    && install --mode=0644 /tmp/bgutil-ytdlp-pot-provider.zip \
        /usr/local/share/sooqa/yt-dlp-plugins/bgutil-ytdlp-pot-provider-${BGUTIL_VERSION}.zip \
    && yt-dlp --version \
    && deno --version \
    && yt-dlp --help | grep --fixed-strings -- "--js-runtimes" >/dev/null \
    && yt-dlp --help | grep --fixed-strings -- "--no-remote-components" >/dev/null \
    && mkdir -p /var/lib/sooqa/work \
    && chown -R sooqa:sooqa /var/lib/sooqa \
    && rm -f /tmp/yt-dlp /tmp/deno.zip /tmp/bgutil-ytdlp-pot-provider.zip \
    && apt-get purge --yes curl unzip \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/sooqa/target/release/sooqa-server /usr/local/bin/sooqa-server
COPY --from=builder /usr/src/sooqa/target/release/sooqa-worker /usr/local/bin/sooqa-worker
COPY --from=builder /usr/src/sooqa/target/release/sooqa-companion /usr/local/bin/sooqa-companion

USER sooqa
ENTRYPOINT ["sooqa-server"]
