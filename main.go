package main

import (
	"fmt"
	"io/ioutil"
	"k8s.io/client-go/rest"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

type Controller struct {
	indexer  cache.Indexer  // Indexer 的引用
	queue    workqueue.RateLimitingInterface  //workqueue 的引用
	informer cache.Controller // Informer 的引用
}

func NewController(indexer cache.Indexer, queue workqueue.RateLimitingInterface, informer cache.Controller) *Controller {
	return &Controller{indexer, queue, informer}
}

func (c *Controller) Run(threadiness int, stopCh chan struct{}) {
	defer runtime.HandleCrash()

	defer c.queue.ShutDown()

	klog.Info("Starting EndPoints Controller")

	go c.informer.Run(stopCh)   // 启动 informer

	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Time out waitng for caches to sync"))
		return
	}

	// 启动多个 worker 处理 workqueue 中的对象
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	klog.Info("Stopping EndPoints controller")
}

func (c *Controller) runWorker() {
	// 启动无限循环，接收并处理消息
	for c.processNextItem() {

	}
}

// 从 workqueue 中获取对象，并打印信息。
func (c *Controller) processNextItem() bool {
	key, shutdown := c.queue.Get()
	// 退出
	if shutdown {
		return false
	}

	// 标记此key已经处理
	defer c.queue.Done(key)

	// 将key对应的 object 的信息进行打印
	err := c.syncToStdout(key.(string))

	c.handleError(err, key)
	return true
}

// 获取 key 对应的 object，并打印相关信息
func (c *Controller) syncToStdout(key string) error {
	obj, exists, err := c.indexer.GetByKey(key)
	if err != nil {
		klog.Errorf("Fetching object with key %s from store failed with %v", key, err)
		return err
	}
	if !exists {
		fmt.Printf("EndPoints %s does not exist")
	} else {
		fmt.Printf("Sync/Add/Update for EndPoints %s\n", obj.(*v1.Endpoints).GetName())
	}
	return nil
}

func (c *Controller) handleError(err error, key interface{}) {
	fmt.Println(err, key)
}

// 获取k8s restful client配置
func GetRestConf() (restConf *rest.Config, err error) {
	var (
		kubeconfig []byte
	)

	// 读kubeconfig文件
	if kubeconfig, err = ioutil.ReadFile("./admin.conf"); err != nil {
		goto END
	}
	// 生成rest client配置
	if restConf, err = clientcmd.RESTConfigFromKubeConfig(kubeconfig); err != nil {
		goto END
	}
END:
	return
}

func main() {
	var (
		config *rest.Config
		err error
	)

	// 获取Config信息
	if config, err = GetRestConf(); err != nil {
		klog.Fatal(err)
	}

	// 创建 k8s client
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatal(err)
	}

	// 指定 ListWatcher 在所有namespace下监听 pod 资源
	epListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "endpoints", v1.NamespaceAll, fields.Everything())

	// 创建 workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// 创建 indexer 和 informer
	indexer, informer := cache.NewIndexerInformer(epListWatcher, &v1.Endpoints{}, 0, cache.ResourceEventHandlerFuncs{
		// 当有 pod 创建时，根据 Delta queue 弹出的 object 生成对应的Key，并加入到 workqueue中。此处可以根据Object的一些属性，进行过滤
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		// pod 更新操作
		UpdateFunc: func(oldobj, newobj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newobj)
			if err == nil {
				queue.Add(key)
			}
		},
		// pod 删除操作
		DeleteFunc: func(obj interface{}) {
			// DeletionHandlingMetaNamespaceKeyFunc 会在生成key 之前检查。因为资源删除后有可能会进行重建等操作，监听时错过了删除信息，从而导致该条记录是陈旧的。
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	}, cache.Indexers{})

	controller := NewController(indexer, queue, informer)

	stop := make(chan struct{})

	defer close(stop)
	// 启动 controller
	go controller.Run(1, stop)

	select {}
}