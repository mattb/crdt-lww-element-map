package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/weaveworks/mesh"
)

func main() {
	peers := &stringset{}
	var (
		httpListen = flag.String("http", ":8080", "HTTP listen address")
		meshListen = flag.String("mesh", net.JoinHostPort("0.0.0.0", strconv.Itoa(mesh.Port)), "mesh listen address")
		hwaddr     = flag.String("hwaddr", mustHardwareAddr(), "MAC address, i.e. mesh peer ID")
		nickname   = flag.String("nickname", mustHostname(), "peer nickname")
		password   = flag.String("password", "", "password (optional)")
		channel    = flag.String("channel", "default", "gossip channel name")
	)
	flag.Var(peers, "peer", "initial peer (may be repeated)")
	flag.Parse()

	logger := log.New(os.Stderr, *nickname+"> ", log.LstdFlags)
	//logger := log.New(ioutil.Discard, "", 0)

	host, portStr, err := net.SplitHostPort(*meshListen)
	if err != nil {
		logger.Fatalf("mesh address: %s: %v", *meshListen, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		logger.Fatalf("mesh address: %s: %v", *meshListen, err)
	}

	name, err := mesh.PeerNameFromString(*hwaddr)
	if err != nil {
		logger.Fatalf("%s: %v", *hwaddr, err)
	}

	router, err := mesh.NewRouter(mesh.Config{
		Host:               host,
		Port:               port,
		ProtocolMinVersion: mesh.ProtocolMinVersion,
		Password:           []byte(*password),
		ConnLimit:          64,
		PeerDiscovery:      false,
		TrustedSubnets:     []*net.IPNet{},
	}, name, *nickname, mesh.NullOverlay{}, log.New(ioutil.Discard, "", 0))

	if err != nil {
		logger.Fatalf("Could not create router: %v", err)
	}

	peer := newPeer(name, logger)
	gossip, err := router.NewGossip(*channel, peer)
	if err != nil {
		logger.Fatalf("Could not create gossip: %v", err)
	}

	peer.register(gossip)

	func() {
		logger.Printf("mesh router starting (%s)", *meshListen)
		router.Start()
	}()
	defer func() {
		logger.Printf("mesh router stopping")
		router.Stop()
	}()

	router.ConnectionMaker.InitiateConnections(peers.slice(), true)

	errs := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT)
		errs <- fmt.Errorf("%s", <-c)
	}()
	go func() {
		logger.Printf("HTTP server starting (%s)", *httpListen)
		http.HandleFunc("/", handle(peer))
		errs <- http.ListenAndServe(*httpListen, nil)
	}()
	logger.Print(<-errs)
}

type entries interface {
	get(key string) (string, bool)
	getAll() map[string]string
	set(key string, value string) string
	del(key string)
}

func handle(c entries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			k := r.FormValue("k")
			if k != "" {
				got, _ := c.get(r.FormValue("k"))
				fmt.Fprintf(w, "get => %d\n", got)
			} else {
				fmt.Fprintf(w, "%v\n", c.getAll())
			}
		case "POST":
			fmt.Fprintf(w, "set => %d\n", c.set(r.FormValue("k"), r.FormValue("v")))
		case "DELETE":
			k := r.FormValue("k")
			c.del(k)
			fmt.Fprintf(w, "delete => %d\n", k)
		}
	}
}

type stringset map[string]struct{}

func (ss stringset) Set(value string) error {
	ss[value] = struct{}{}
	return nil
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}

func mustHardwareAddr() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, iface := range ifaces {
		if s := iface.HardwareAddr.String(); s != "" {
			return s
		}
	}
	panic("no valid network interfaces")
}

func mustHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}
