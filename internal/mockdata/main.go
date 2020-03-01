package mockdata

//go:generate mockgen -destination=mock.go -package=$GOPACKAGE github.com/rueian/groupcache/pkg/data Value,Store,Loader
