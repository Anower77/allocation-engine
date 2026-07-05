package temporal

import (
	"go.temporal.io/sdk/client"
)

// NewClient dials the Temporal frontend. hostPort is e.g. "temporal:7233".
func NewClient(hostPort string) (client.Client, error) {
	return client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: "default",
	})
}
