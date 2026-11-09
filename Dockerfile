FROM eclipse-temurin:21-jdk-jammy AS build-env
WORKDIR /app/pricing-engine
COPY pricing-engine/ .
RUN ./mvnw clean package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build-env /app/pricing-engine/target/pricing-engine-0.0.1-SNAPSHOT.jar ./

CMD ["java", "-jar", "pricing-engine-0.0.1-SNAPSHOT.jar"]
