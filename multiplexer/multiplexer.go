package multiplexer

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ERR_EXIT = errors.New("exit")
)

type Socket interface {
	// read from socket
	Read() ([]byte, error)
	// write to socket
	Write([]byte) error
	// close socket
	Close()
}

type DataHandler interface {
	// process data
	Process([]byte)
}

type ErrorHandler interface {
	// handle errors
	OnError(error)
}

type IdentityHandler interface {
	// get id from datapackage
	GetIdentity([]byte) uint32
	// set id to datapackage
	SetIdentity([]byte, uint32)
}

type Connection struct {
	conn  Socket
	wg    sync.WaitGroup
	mutex sync.Mutex
	// 请求者表，每个请求者的到来都会被分配一个unit32类型的ID作为KEY，和从chch池中取数据通信通道作为VALUE
	applicants map[uint32]chan []byte
	chexit     chan bool        // 关闭连接信号
	chsend     chan []byte      // 数据通信通道
	chch       chan chan []byte // 数据通信通道池
	dh         DataHandler      // 数据处理handler
	ih         IdentityHandler  // 从数据设置、获取ID的handler
	eh         ErrorHandler     // 错误handler
	identity   uint32           // 最新的请求者ID，每一个ID都是在此基础上自增1，我也不知道溢出以后会咋样、、、
}

func NewConnection(conn Socket, maxcount int, dh DataHandler, ih IdentityHandler, eh ErrorHandler) *Connection {
	// 最大请求者队列长度
	count := maxcount
	// 如果不足1024仍然设为1024，不缺内存就是这么任性
	if count < 1024 {
		count = 1024
	}

	// 为可能到达的请求者准备数据传输缓冲池
	chch := make(chan chan []byte, count)
	for i := 0; i < count; i++ {
		chch <- make(chan []byte, 1)
	}
	return &Connection{
		conn:       conn,
		applicants: make(map[uint32]chan []byte, count), // 足够容纳最大请求者数量的表
		chsend:     make(chan []byte, count),
		chexit:     make(chan bool),
		chch:       chch,
		dh:         dh,
		ih:         ih,
		eh:         eh,
	}
}

func (p *Connection) Start() {
	// 用于阻塞
	p.wg.Add(2)
	// 开启接收socket传入数据的服务
	go func() {
		defer p.wg.Done()
		p.recv()
	}()
	// 开启向socket发送数据的服务
	go func() {
		defer p.wg.Done()
		p.send()
	}()
}

func (p *Connection) Close() {
	// 关闭chexit channel，所有ehexit将不再阻塞
	close(p.chexit)
	// 关闭socket连接
	p.conn.Close()
	// 确保recv和send服务都已经完成关闭
	p.wg.Wait()
}

// 发起请求并返回结果
func (p *Connection) Query(data []byte) (res []byte, err error) {
	var ch chan []byte
	select {
	case <-p.chexit:
		return nil, ERR_EXIT
	case ch = <-p.chch: //从数据通信通道池中获取一个通道，给请求者使用
		defer func() {
			//请求完成后，无论失败与否都要将通道返还给通道池
			p.chch <- ch
		}()
	}
	// 新生成一个ID作为请求者ID
	id := p.newIdentity()
	// 将请求者ID添加到响应报文中
	p.ih.SetIdentity(data, id)
	// 将请求者加入到请求者表
	p.addApplicant(id, ch)
	defer func() {
		//若请求过程发生错误，则将该请求者从表中除去
		if err != nil {
			p.popApplicant(id)
		}
	}()

	//发送带有请求者ID的数据，这样一来，就可以通过ID在请求者表中获取正确的ch(channel都是引用传值的)，将反馈信息传入进去
	if err := p.Write(data); err != nil {
		return nil, err
	}
	select {
	case <-p.chexit:
		return nil, ERR_EXIT
	case res = <-ch: //此时会阻塞，直到信息反馈
		break
	}
	return res, nil
}

// 对已知的请求进行回复，其实就是发送带ID的数据报文
func (p *Connection) Reply(query, answer []byte) error {
	/* put back the identity attached to the query - start */

	// 从请求报文中获取请求者ID
	id := p.ih.GetIdentity(query)
	// 将请求者ID添加到响应报文中
	p.ih.SetIdentity(answer, id)

	/* put back the identity attached to the query - end*/

	// 向Socket发送响应报文
	return p.Write(answer)
}

// 单纯进行数据发送
func (p *Connection) Write(data []byte) error {
	select {
	case <-p.chexit:
		return ERR_EXIT
	case p.chsend <- data: // 将待发送的数据送入数据发送通道
		break
	}
	return nil
}

func (p *Connection) send() {
	//不断循环
	for {
		select {
		//如果chexit被关闭，退出服务
		case <-p.chexit:
			return
		case data := <-p.chsend: //一旦有数据需要发送，马上往socket中写入
			if p.conn.Write(data) != nil {
				return
			}
		}
	}
}

func (p *Connection) recv() (err error) {
	defer func() {
		//错误处理
		if err != nil {
			select {
			case <-p.chexit:
				err = nil
			default:
				//调用errorHandler的OnError
				p.eh.OnError(err)
			}
		}
	}()
	//不断循环
	for {
		//如果chexit被关闭，退出服务
		select {
		case <-p.chexit:
			return nil
		default:
			break
		}
		//等待有数据传入
		data, err := p.conn.Read()
		if err != nil {
			return err
		}
		//从数据报文中获取请求者ID，如果ID大于0，说明这是某一个请求的反馈信息
		if id := p.ih.GetIdentity(data); id > 0 {
			//根据ID获取到对应的channel
			ch, ok := p.popApplicant(id)
			if ok {
				//将数据发送给channel
				ch <- data
				//跳过数据处理过程，进入下一个监听循环
				continue
			}
		}

		//对数据报文进行处理
		p.dh.Process(data)
	}
	return nil
}

func (p *Connection) newIdentity() uint32 {
	//原子操作+1
	return atomic.AddUint32(&p.identity, 1)
}

// 增加请求者
func (p *Connection) addApplicant(identity uint32, ch chan []byte) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.applicants[identity] = ch
}

// 获取请求者
func (p *Connection) popApplicant(identity uint32) (chan []byte, bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	ch, ok := p.applicants[identity]
	if !ok {
		return nil, false
	}
	delete(p.applicants, identity)
	return ch, true
}
