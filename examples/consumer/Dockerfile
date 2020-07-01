FROM alpine:3.10
# COPY --from=builder /go/bin/consumer /app/
COPY build/consumer /app/consumer
CMD ["/app/consumer"]