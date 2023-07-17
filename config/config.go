package config

type RespCmdServiceOptions struct {
	Addr                  string `mapstructure:"addr"`
	AuthPassword          string `mapstructure:"authPassword"`
	ConnKeepaliveInterval int    `mapstructure:"connKeepaliveInterval"`
}

func DefaultRespCmdServiceOptions() *RespCmdServiceOptions {
	return &RespCmdServiceOptions{
		Addr: "127.0.0.1:6666",
		//defualt 0 disable and not check
		ConnKeepaliveInterval: 0,
	}
}
