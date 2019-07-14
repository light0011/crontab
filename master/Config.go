package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Config struct {
	ApiPort string	`json:"apiPort"`
	ApiReadTimeout int	`json:"apiReadTimeout"`
	ApiWriteTimeout int	`json:"apiWriteTimeout"`
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	WebRoot string `json:"webroot"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
}


var G_config *Config

func InitConfig(filename string) error  {
	var (
		content []byte
		conf *Config
		err error

	)
	conf = new(Config)
	//读取配置文件内容
	if content,err = ioutil.ReadFile(filename);err !=nil {
		return err
	}
	if err = json.Unmarshal(content,conf);err != nil {
		return err
	}
	fmt.Println(conf)

	G_config = conf

	return nil

}