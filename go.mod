module juno

go 1.18

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5

require (
	github.com/BurntSushi/toml v1.1.0
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/satori/go.uuid v1.2.0
	github.com/signalfx/golib/v3 v3.3.45
	github.com/spaolacci/murmur3 v1.1.0
	go.etcd.io/etcd/client/v3 v3.5.4
	go.opentelemetry.io/otel v1.11.2
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v0.34.0
	go.opentelemetry.io/otel/metric v0.34.0
	go.opentelemetry.io/otel/sdk v1.11.2
	go.opentelemetry.io/otel/sdk/metric v0.34.0
	go.opentelemetry.io/proto/otlp v0.19.0
	google.golang.org/protobuf v1.28.1
	golang.org/x/net v0.7.0
)

require (
	github.com/cenkalti/backoff/v4 v4.2.0 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/go-logfmt/logfmt v0.5.0 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.7.0 // indirect
	github.com/jaegertracing/jaeger v1.26.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/signalfx/com_signalfx_metrics_protobuf v0.0.2 // indirect
	github.com/signalfx/gohistogram v0.0.0-20160107210732-1ccfd2ff5083 // indirect
	github.com/signalfx/sapm-proto v0.7.2 // indirect
	go.etcd.io/etcd/api/v3 v3.5.4 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.4 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.11.2 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.34.0 // indirect
	go.opentelemetry.io/otel/trace v1.11.2 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.19.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	google.golang.org/grpc v1.46.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)
