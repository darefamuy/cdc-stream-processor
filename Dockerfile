# =============================================================================
# AB Bank Kafka Streams Application
# Multi-stage build: Gradle build → lean JRE runtime image
# =============================================================================

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM gradle:8.7-jdk17 AS builder
LABEL stage=builder

WORKDIR /app
COPY settings.gradle build.gradle gradle.properties ./
# Cache dependencies layer separately from source
RUN gradle dependencies --no-daemon -q || true

COPY src ./src
RUN gradle shadowJar --no-daemon -x test

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-jammy AS runtime

LABEL maintainer="AB Bank Engineering <platform@abbank.com>"
LABEL description="AB Bank Kafka Streams — real-time transaction processing"
LABEL version="1.0.0"

# Add non-root user
RUN groupadd -r streams && useradd -r -g streams streams

WORKDIR /app
COPY --from=builder /app/build/libs/cdc-stream-processor-*.jar cdc-stream-processor.jar

# State stores directory (mounted as volume for persistence)
RUN mkdir -p /var/lib/streams-state && chown streams:streams /var/lib/streams-state

USER streams

EXPOSE 8080

# JVM tuning for containerised environments
ENV JAVA_OPTS="-XX:+UseContainerSupport \
               -XX:MaxRAMPercentage=75.0 \
               -XX:InitialRAMPercentage=50.0 \
               -XX:+ExitOnOutOfMemoryError \
               -XX:+HeapDumpOnOutOfMemoryError \
               -Djava.security.egd=file:/dev/./urandom \
               -Dkafka.streams.state.dir=/var/lib/streams-state"

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -sf http://localhost:8080/health || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar cdc-stream-processor.jar"]
