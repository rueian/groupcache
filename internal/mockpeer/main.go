package mockpeer

//go:generate mockgen -destination=mock.go -package=$GOPACKAGE github.com/rueian/groupcache/pkg/peer Peer,Picker
