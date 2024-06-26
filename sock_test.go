package goczmq

import (
	"encoding/gob"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSendFrame(t *testing.T) {
	pushSock := NewSock(Push)
	defer pushSock.Destroy()

	pullSock := NewSock(Pull)
	defer pullSock.Destroy()

	_, err := pullSock.Bind("inproc://test-send-frame")
	require.NoError(t, err)

	err = pushSock.Connect("inproc://test-send-frame")
	require.NoError(t, err)

	err = pushSock.SendFrame([]byte("Hello"), FlagNone)
	require.NoError(t, err)

	frame, _, err := pullSock.RecvFrame()
	require.NoError(t, err)

	if want, have := "Hello", string(frame); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	_, flag, err := pullSock.RecvFrameNoWait()
	require.Error(t, err)

	if want, have := true, flag == 0; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	err = pushSock.SendFrame([]byte("World"), FlagNone)
	require.NoError(t, err)

	_, flag, err = pullSock.RecvFrameNoWait()
	require.NoError(t, err)

	if want, have := true, flag == 0; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	err = pushSock.SendFrame([]byte("World"), FlagNone)
	require.NoError(t, err)
}

func TestSendEmptyFrame(t *testing.T) {
	pushSock := NewSock(Push)
	defer pushSock.Destroy()

	pullSock := NewSock(Pull)
	defer pullSock.Destroy()

	_, err := pullSock.Bind("inproc://test-send-empty")
	require.NoError(t, err)

	err = pushSock.Connect("inproc://test-send-empty")
	require.NoError(t, err)

	var empty []byte
	err = pushSock.SendFrame(empty, FlagNone)
	require.NoError(t, err)

	frame, _, err := pullSock.RecvFrame()
	require.NoError(t, err)

	if want, have := 0, len(frame); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestSendMessage(t *testing.T) {
	pushSock := NewSock(Push)
	defer pushSock.Destroy()

	pullSock := NewSock(Pull)
	defer pullSock.Destroy()

	_, err := pullSock.Bind("inproc://test-send-msg")
	require.NoError(t, err)

	err = pushSock.Connect("inproc://test-send-msg")
	require.NoError(t, err)

	err = pushSock.SendMessage([][]byte{[]byte("Hello")})
	require.NoError(t, err)

	msg, err := pullSock.RecvMessage()
	require.NoError(t, err)

	if want, have := "Hello", string(msg[0]); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	_, err = pullSock.RecvMessageNoWait()
	if err == nil {
		t.Error(err)
	}

	err = pushSock.SendMessage([][]byte{[]byte("World")})
	require.NoError(t, err)

	msg, err = pullSock.RecvMessageNoWait()
	require.NoError(t, err)

	if want, have := "World", string(msg[0]); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestPubSub(t *testing.T) {
	bogusPub, err := NewPub("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusPub.Destroy()

	bogusSub, err := NewSub("bogus://bogus", "")
	if err == nil {
		t.Error(err)
	}
	defer bogusSub.Destroy()

	pub, err := NewPub("inproc://pub1,inproc://pub2")
	require.NoError(t, err)
	defer pub.Destroy()

	sub, err := NewSub("inproc://pub1,inproc://pub2", "")
	require.NoError(t, err)
	defer sub.Destroy()

	err = pub.SendFrame([]byte("test pub sub"), FlagNone)
	require.NoError(t, err)

	frame, _, err := sub.RecvFrame()
	require.NoError(t, err)

	if want, have := "test pub sub", string(frame); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestReqRep(t *testing.T) {
	bogusReq, err := NewReq("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusReq.Destroy()

	bogusRep, err := NewRep("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusRep.Destroy()

	rep, err := NewRep("inproc://rep1,inproc://rep2")
	require.NoError(t, err)
	defer rep.Destroy()

	req, err := NewReq("inproc://rep1,inproc://rep2")
	require.NoError(t, err)
	defer req.Destroy()

	err = req.SendFrame([]byte("Hello"), FlagNone)
	require.NoError(t, err)

	reqframe, _, err := rep.RecvFrame()
	require.NoError(t, err)

	if want, have := "Hello", string(reqframe); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	err = rep.SendFrame([]byte("World"), FlagNone)
	require.NoError(t, err)

	repframe, _, err := req.RecvFrame()
	require.NoError(t, err)

	if want, have := "World", string(repframe); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestPushPull(t *testing.T) {
	bogusPush, err := NewPush("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusPush.Destroy()

	bogusPull, err := NewPull("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusPull.Destroy()

	push, err := NewPush("inproc://push1,inproc://push2")
	require.NoError(t, err)
	defer push.Destroy()

	pull, err := NewPull("inproc://push1,inproc://push2")
	require.NoError(t, err)
	defer pull.Destroy()

	err = push.SendFrame([]byte("Hello"), FlagMore)
	require.NoError(t, err)

	err = push.SendFrame([]byte("World"), FlagNone)
	require.NoError(t, err)

	msg, err := pull.RecvMessage()
	require.NoError(t, err)

	if want, have := "Hello", string(msg[0]); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := "World", string(msg[1]); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestRouterDealer(t *testing.T) {
	bogusDealer, err := NewDealer("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusDealer.Destroy()

	bogusRouter, err := NewRouter("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusRouter.Destroy()

	dealer, err := NewDealer("inproc://router1,inproc://router2")
	require.NoError(t, err)
	defer dealer.Destroy()

	router, err := NewRouter("inproc://router1,inproc://router2")
	require.NoError(t, err)
	defer router.Destroy()

	err = dealer.SendFrame([]byte("Hello"), FlagNone)
	require.NoError(t, err)

	msg, err := router.RecvMessage()
	require.NoError(t, err)

	if want, have := 2, len(msg); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := "Hello", string(msg[1]); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	msg[1] = []byte("World")

	err = router.SendMessage(msg)
	require.NoError(t, err)

	msg, err = dealer.RecvMessage()
	require.NoError(t, err)

	if want, have := 1, len(msg); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := "World", string(msg[0]); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestXSubXPub(t *testing.T) {
	bogusXPub, err := NewXPub("bogus://bogus")
	if err == nil {
		t.Error("NewXPub should have returned error and did not")
	}
	defer bogusXPub.Destroy()

	bogusXSub, err := NewXSub("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusXSub.Destroy()

	xpub, err := NewXPub("inproc://xpub1,inproc://xpub2")
	require.NoError(t, err)
	defer xpub.Destroy()

	xsub, err := NewXSub("inproc://xpub1,inproc://xpub2")
	require.NoError(t, err)
	defer xsub.Destroy()
}

func TestPair(t *testing.T) {
	bogusPair, err := NewPair("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusPair.Destroy()

	pair1, err := NewPair(">inproc://pair")
	require.NoError(t, err)
	defer pair1.Destroy()

	pair2, err := NewPair("@inproc://pair")
	require.NoError(t, err)
	defer pair2.Destroy()
}

func TestStream(t *testing.T) {
	bogusStream, err := NewStream("bogus://bogus")
	if err == nil {
		t.Error(err)
	}
	defer bogusStream.Destroy()

	stream1, err := NewStream(">inproc://stream")
	require.NoError(t, err)
	defer stream1.Destroy()

	stream2, err := NewStream("@inproc://stream")
	require.NoError(t, err)
	defer stream2.Destroy()

}

func TestPollin(t *testing.T) {
	push, err := NewPush("inproc://pollin")
	require.NoError(t, err)
	defer push.Destroy()

	pull, err := NewPull("inproc://pollin")
	require.NoError(t, err)
	defer pull.Destroy()

	if want, have := false, pull.Pollin(); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	err = push.SendFrame([]byte("Hello World"), FlagNone)
	require.NoError(t, err)

	if want, have := true, pull.Pollin(); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestPollinPolloutRouter(t *testing.T) {
	router, err := NewRouter("inproc://router")
	require.NoError(t, err)
	defer router.Destroy()

	dealer, err := NewDealer("inproc://router")
	require.NoError(t, err)
	defer dealer.Destroy()

	// for Router pollout is always true
	if want, have := true, router.Pollout(); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := false, router.Pollin(); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	err = dealer.SendFrame([]byte("Hello World"), FlagNone)
	require.NoError(t, err)

	if want, have := true, router.Pollin(); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := true, router.Pollout(); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestPollout(t *testing.T) {
	push := NewSock(Push)
	_, err := push.Bind("inproc://pollout")
	require.NoError(t, err)
	defer push.Destroy()

	if want, have := false, push.Pollout(); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	pull := NewSock(Pull)
	defer pull.Destroy()

	err = pull.Connect("inproc://pollout")
	require.NoError(t, err)

	if want, have := true, push.Pollout(); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestReader(t *testing.T) {
	pushSock := NewSock(Push)
	defer pushSock.Destroy()

	pullSock := NewSock(Pull)
	defer pullSock.Destroy()

	_, err := pullSock.Bind("inproc://test-read")
	require.NoError(t, err)

	err = pushSock.Connect("inproc://test-read")
	require.NoError(t, err)

	err = pushSock.SendFrame([]byte("Hello"), FlagNone)
	require.NoError(t, err)

	b := make([]byte, 5)

	n, err := pullSock.Read(b)

	if want, have := 5, n; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	require.NoError(t, err)

	if want, have := "Hello", string(b); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	err = pushSock.SendFrame([]byte("Hello"), FlagMore)
	require.NoError(t, err)

	err = pushSock.SendFrame([]byte(" World"), FlagNone)
	require.NoError(t, err)

	b = make([]byte, 8)
	_, err = pullSock.Read(b)
	if want, have := ErrSliceFull, err; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := "Hello Wo", string(b); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestReaderWithRouterDealer(t *testing.T) {
	dealerSock := NewSock(Dealer)
	defer dealerSock.Destroy()

	routerSock := NewSock(Router)
	defer routerSock.Destroy()

	_, err := routerSock.Bind("inproc://test-read-router")
	require.NoError(t, err)

	err = dealerSock.Connect("inproc://test-read-router")
	require.NoError(t, err)

	err = dealerSock.SendFrame([]byte("Hello"), FlagNone)
	require.NoError(t, err)

	b := make([]byte, 5)

	n, err := routerSock.Read(b)
	require.NoError(t, err)

	if want, have := 5, n; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := "Hello", string(b); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	err = dealerSock.SendFrame([]byte("Hello"), FlagMore)
	require.NoError(t, err)

	err = dealerSock.SendFrame([]byte(" World"), FlagNone)
	require.NoError(t, err)

	b = make([]byte, 8)
	_, err = routerSock.Read(b)

	if want, have := ErrSliceFull, err; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := "Hello Wo", string(b); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	n, err = routerSock.Write([]byte("World"))
	require.NoError(t, err)

	if want, have := 5, n; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	frame, _, err := dealerSock.RecvFrame()
	require.NoError(t, err)

	if want, have := "World", string(frame); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestReaderWithRouterDealerAsync(t *testing.T) {
	dealerSock1 := NewSock(Dealer)
	defer dealerSock1.Destroy()

	dealerSock2 := NewSock(Dealer)
	defer dealerSock2.Destroy()

	routerSock := NewSock(Router)
	defer routerSock.Destroy()

	_, err := routerSock.Bind("inproc://test-read-router-async")
	require.NoError(t, err)

	err = dealerSock1.Connect("inproc://test-read-router-async")
	require.NoError(t, err)

	err = dealerSock1.SendFrame([]byte("Hello From Client 1!"), FlagNone)
	require.NoError(t, err)

	err = dealerSock2.Connect("inproc://test-read-router-async")
	require.NoError(t, err)

	err = dealerSock2.SendFrame([]byte("Hello From Client 2!"), FlagNone)
	require.NoError(t, err)

	msg := make([]byte, 255)

	n, err := routerSock.Read(msg)
	require.NoError(t, err)

	if want, have := 20, n; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	client1ID := routerSock.GetLastClientID()

	if want, have := 20, n; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := "Hello From Client 1!", string(msg[:n]); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	n, err = routerSock.Read(msg)
	require.NoError(t, err)

	if want, have := 20, n; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	client2ID := routerSock.GetLastClientID()

	if want, have := "Hello From Client 2!", string(msg[:n]); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	routerSock.SetLastClientID(client1ID)
	_, err = routerSock.Write([]byte("Hello Client 1!"))

	require.NoError(t, err)

	frame, _, err := dealerSock1.RecvFrame()
	require.NoError(t, err)

	if want, have := "Hello Client 1!", string(frame); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	routerSock.SetLastClientID(client2ID)
	_, err = routerSock.Write([]byte("Hello Client 2!"))

	require.NoError(t, err)

	frame, _, err = dealerSock2.RecvFrame()
	require.NoError(t, err)

	if want, have := "Hello Client 2!", string(frame); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

type encodeMessage struct {
	Foo string
	Bar []byte
	Bat int
}

func TestPushPullEncodeDecode(t *testing.T) {
	push, err := NewPush("inproc://pushpullencode")
	require.NoError(t, err)
	defer push.Destroy()

	pull, err := NewPull("inproc://pushpullencode")
	require.NoError(t, err)
	defer pull.Destroy()

	encoder := gob.NewEncoder(push)
	decoder := gob.NewDecoder(pull)

	sent := encodeMessage{
		Foo: "the answer",
		Bar: []byte("is"),
		Bat: 42,
	}

	err = encoder.Encode(sent)
	require.NoError(t, err)

	var received encodeMessage
	err = decoder.Decode(&received)
	require.NoError(t, err)

	if want, have := received.Foo, sent.Foo; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := string(received.Bar), string(sent.Bar); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := received.Bat, sent.Bat; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if received.Bat != sent.Bat {
		t.Errorf("expected %#v, have %#v", sent.Bat, received.Bat)
	}
}

func TestDealerRouterEncodeDecode(t *testing.T) {
	router, err := NewRouter("inproc://dealerrouterencode")
	require.NoError(t, err)
	defer router.Destroy()

	dealer, err := NewDealer("inproc://dealerrouterencode")
	require.NoError(t, err)
	defer dealer.Destroy()

	rencoder := gob.NewEncoder(router)
	rdecoder := gob.NewDecoder(router)

	dencoder := gob.NewEncoder(dealer)
	ddecoder := gob.NewDecoder(dealer)

	question := encodeMessage{
		Foo: "what is",
		Bar: []byte("the answer"),
		Bat: 0,
	}

	err = dencoder.Encode(question)
	require.NoError(t, err)

	var received encodeMessage
	err = rdecoder.Decode(&received)
	require.NoError(t, err)

	if want, have := received.Foo, question.Foo; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := string(received.Bar), string(question.Bar); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := received.Bat, question.Bat; want != have {
		t.Errorf("expected %#v, have %#v", want, have)
	}

	sent := encodeMessage{
		Foo: "the answer",
		Bar: []byte("is"),
		Bat: 42,
	}

	err = rencoder.Encode(sent)
	require.NoError(t, err)

	var answer encodeMessage
	err = ddecoder.Decode(&answer)
	require.NoError(t, err)

	if want, have := answer.Foo, sent.Foo; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := string(answer.Bar), string(sent.Bar); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	if want, have := answer.Bat, sent.Bat; want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}
}

func TestRecvFrameCalledAfterDestroy(t *testing.T) {
	rep, err := NewRep("inproc://rep1")
	require.NoError(t, err)

	req, err := NewReq("inproc://rep1")
	require.NoError(t, err)

	defer req.Destroy()

	err = req.SendFrame([]byte("Hello"), FlagNone)
	require.NoError(t, err)

	// Just verify that the connection actually works.
	reqframe, _, err := rep.RecvFrame()
	require.NoError(t, err)

	if want, have := "Hello", string(reqframe); want != have {
		t.Errorf("want %#v, have %#v", want, have)
	}

	rep.Destroy()
	_, _, err = rep.RecvFrame()
	if err != ErrRecvFrameAfterDestroy {
		t.Errorf("want %#v, have %#v", ErrRecvFrameAfterDestroy, err)
	}

}

func ExampleSock_output() {
	// create dealer socket
	dealer, err := NewDealer("inproc://example")
	if err != nil {
		panic(err)
	}
	defer dealer.Destroy()

	// create router socket
	router, err := NewRouter("inproc://example")
	if err != nil {
		panic(err)
	}
	defer router.Destroy()

	// send hello message
	err = dealer.SendFrame([]byte("Hello"), FlagNone)
	if err != nil {
		panic(err)
	}

	// receive hello message
	request, err := router.RecvMessage()
	if err != nil {
		panic(err)
	}

	// first frame is identify of client - let's append 'World'
	// to the message and route it back.
	request = append(request, []byte("World"))

	// send reply
	err = router.SendMessage(request)
	if err != nil {
		panic(err)
	}

	// receive reply
	reply, err := dealer.RecvMessage()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s %s", string(reply[0]), string(reply[1]))
	// Output: Hello World
}

func benchmarkSockSendFrame(size int, b *testing.B) {
	pullSock := NewSock(Pull)
	defer pullSock.Destroy()

	_, err := pullSock.Bind(fmt.Sprintf("inproc://benchSockSendFrame%#v", size))
	if err != nil {
		panic(err)
	}

	go func() {
		pushSock := NewSock(Push)
		defer pushSock.Destroy()
		err := pushSock.Connect(fmt.Sprintf("inproc://benchSockSendFrame%#v", size))
		if err != nil {
			panic(err)
		}

		payload := make([]byte, size)
		for i := 0; i < b.N; i++ {
			err = pushSock.SendFrame(payload, FlagNone)
			if err != nil {
				panic(err)
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		msg, _, err := pullSock.RecvFrame()
		if err != nil {
			panic(err)
		}
		if len(msg) != size {
			panic("msg too small")
		}

		b.SetBytes(int64(size))
	}
}

func BenchmarkSockSendFrame1k(b *testing.B)  { benchmarkSockSendFrame(1024, b) }
func BenchmarkSockSendFrame4k(b *testing.B)  { benchmarkSockSendFrame(4096, b) }
func BenchmarkSockSendFrame16k(b *testing.B) { benchmarkSockSendFrame(16384, b) }

func BenchmarkEncodeDecode(b *testing.B) {
	pullSock := NewSock(Pull)
	defer pullSock.Destroy()

	decoder := gob.NewDecoder(pullSock)

	_, err := pullSock.Bind("inproc://benchSockEncodeDecode")
	if err != nil {
		panic(err)
	}

	go func() {
		pushSock := NewSock(Push)
		defer pushSock.Destroy()
		err := pushSock.Connect("inproc://benchSockEncodeDecode")
		if err != nil {
			panic(err)
		}

		encoder := gob.NewEncoder(pushSock)

		sent := encodeMessage{
			Foo: "the answer",
			Bar: make([]byte, 1024),
			Bat: 42,
		}

		for i := 0; i < b.N; i++ {
			err := encoder.Encode(sent)
			if err != nil {
				panic(err)
			}
		}
	}()

	var received encodeMessage
	for i := 0; i < b.N; i++ {
		err := decoder.Decode(&received)
		if err != nil {
			panic(err)
		}
	}
}
