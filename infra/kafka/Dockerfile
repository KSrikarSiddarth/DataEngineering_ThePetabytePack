# Topic-initialisation image: official Apache Kafka image + our idempotent topic script baked in
# (avoids bind-mounting from the OneDrive folder, which Colima/Docker may not see reliably).
FROM apache/kafka:3.7.1

COPY create-topics.sh /opt/mobilitypulse/create-topics.sh
# strip Windows CRLF line endings (git autocrlf / Windows editors) so bash can run the script
RUN sed -i 's/\r$//' /opt/mobilitypulse/create-topics.sh

ENTRYPOINT ["/bin/bash", "/opt/mobilitypulse/create-topics.sh"]
