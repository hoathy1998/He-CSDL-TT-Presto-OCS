# MinIO đã ngừng phát hành image/binary cộng đồng (quay.io, Docker Hub, dl.min.io trả 410 Gone),
# nên build lại từ mã nguồn chính thức đúng tag phát hành.
FROM golang:1.24-bookworm AS build
ENV CGO_ENABLED=0
RUN go install github.com/minio/minio@RELEASE.2025-04-22T22-12-26Z

FROM debian:bookworm-slim
COPY --from=build /go/bin/minio /usr/local/bin/minio
ENTRYPOINT ["minio"]
