module github.com/go-anyway/framework-elasticsearch

go 1.25.4

require (
	github.com/go-anyway/framework-config v1.0.0
	github.com/go-anyway/framework-log v1.0.0
	github.com/go-anyway/framework-trace v1.0.0
	github.com/elastic/go-elasticsearch/v8 v8.19.1
)

replace (
	github.com/go-anyway/framework-config => ../core/config
	github.com/go-anyway/framework-log => ../core/log
	github.com/go-anyway/framework-trace => ../trace
)
