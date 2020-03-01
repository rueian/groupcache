package cache

import (
	"context"
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/rueian/groupcache/internal/mockdata"
	"github.com/rueian/groupcache/internal/mockpeer"
	"github.com/rueian/groupcache/pkg/flight"
	"github.com/rueian/groupcache/pkg/peer"
	"strconv"
	"testing"
)

func TestLocalLoad(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	picker := mockpeer.NewMockPicker(ctrl)
	loader := mockdata.NewMockLoader(ctrl)
	main := mockdata.NewMockStore(ctrl)
	hot := mockdata.NewMockStore(ctrl)
	ctx := context.Background()
	flights := &flight.Group{}

	main.EXPECT().Bytes().AnyTimes()
	hot.EXPECT().Bytes().AnyTimes()

	group := Group{
		maxBytes: 1000,
		flights:  flights,
		peers:    picker,
		loader:   loader,
		main:     main,
		hot:      hot,
		delegate: 2,
		replicas: 3,
	}

	peers := make([][]peer.Peer, group.replicas)
	for i := range peers {
		peers[i] = make([]peer.Peer, group.delegate)
		for j := range peers[i] {
			peers[i][j] = mockpeer.NewMockPeer(ctrl)
		}
	}

	key := "key"

	// mock pick for replica keys
	for i := 0; i < group.replicas; i++ {
		replica := key + strconv.Itoa(i)
		picker.EXPECT().Pick(replica, group.delegate).Return(peers[i])
	}

	// test all peer fail
	for i := 0; i < group.replicas; i++ {
		peers[i][0].(*mockpeer.MockPeer).EXPECT().Self().Return(false).AnyTimes()
		peers[i][0].(*mockpeer.MockPeer).EXPECT().LookupOrLoad(ctx, key).Return(nil, errors.New("err"))
	}

	// fallback to local
	value := mockdata.NewMockValue(ctrl)
	// local lookup failed twice (before and after single flight lock)
	hot.EXPECT().Get(key).Return(nil).Times(2)
	main.EXPECT().Get(key).Return(nil).Times(2)
	loader.EXPECT().Load(ctx, key).Return(value, nil) // mock get value
	main.EXPECT().Add(key, value)

	for i := 0; i < group.replicas; i++ {
		// mock pick for replica keys
		replica := key + strconv.Itoa(i)
		picker.EXPECT().Pick(replica, 1).Return(peers[i][:1])
		peers[i][0].(*mockpeer.MockPeer).EXPECT().Push(ctx, key, value).Return(nil).AnyTimes()
	}

	res, err := group.Get(context.Background(), key)
	if err != nil {
		t.Error(err)
	}
	if value != res {
		t.Errorf("result not match with %v\n", value)
	}
}
