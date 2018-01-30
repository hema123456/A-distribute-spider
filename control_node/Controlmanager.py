# -*- coding:utf-8 -*-


import time
from control_node.URLmanager import Urlmanager
from control_node.Dataoutput import DataOutput
from multiprocessing.managers import BaseManager
from multiprocessing import Process
from multiprocessing import Queue

__author__ = 'Hippo'


class Controlmanager(object):
    def start_manager(self,url_q,result_q):
        '''
        创建一个分布式管理器
        :param url_q: url队列
        :param result_q: 结果队列
        :return:
        '''
        # 把创建的两个队列注册在网络上，利用register方法，callable参数关联Queue对象
        # 把Queue对象在网络中暴露
        BaseManager.register('get_task_queue',callable=lambda:url_q)
        BaseManager.register('get_result_queue',callable=lambda:result_q)
        #绑定端口 8001，设置口令“baike”,相当于对象初始化
        manager= BaseManager(address=('127.0.0.1', 8888), authkey=b'baike')
        #返回 manager对象
        return manager

    def url_manager_proc(self,url_q,conn_q,root_url):
        url_manager = Urlmanager()
        url_manager.add_new_url(root_url)
        while True:
            while url_manager.has_new_url():
                # 从URL管理器获取新的URL
                new_url = url_manager.get_new_url()
                # 将新的URL发给工作节点
                url_q.put(new_url)
                print('old_url=',url_manager.old_url_size())
                # 加一个循环中止条件，爬取2000个链接后关闭，并保持进度
                if url_manager.old_url_size() > 2000:
                    # 通知爬虫节点工作结束
                    url_q.put('end')
                    print('控制节点发起结束通知')
                    # 关闭管理节点，同时存储set状态
                    url_manager.save_progress('new_urls.txt',url_manager.new_urls)
                    url_manager.save_progress('old_urls.txt',url_manager.old_urls)
                    return
                # 将从result_solve_proc获取到的URL添加到URL管理器
            try:
                if not conn_q.empty():
                    urls = conn_q.get()
                    url_manager.add_new_urls(urls)
            except BaseException as e:
                time.sleep(0.1)

    def result_solve_proc(self,result_q,conn_q,store_q):
        while(True):
            try:
                if not result_q.empty():
                    content = result_q.get(True)
                    if content['new_urls'] == 'end':
                        #结果分析进程接受通知然后结束
                        print('结果分析进程接受通知然后结束！')
                        store_q.put('end')
                        return
                    conn_q.put(content['new_urls'])#url为set类型
                    store_q.put(content['data'])#解析出来的数据为dict类型
                else:
                    time.sleep(0.1)#延时休息
            except Exception:
                time.sleep(0.1)

    def store_proc(self,store_q):
        output = DataOutput()
        while True:
            if not store_q.empty():
                data = store_q.get()
                if data == 'end':
                    print('存储进程接受通知然后结束！')
                    output.output_end(output.filepath)
                    return
                output.store_data(data)
            else:
                time.sleep(0.1)


if __name__ == '__main__':
    """
        url_q: URL管理进程将URL传递给爬虫节点的通道
        result_q: 爬虫节点将数据返回给数据提取进程的通道
        conn_q: 数据提取进程将新的URL数据提交给URL管理进程的通道
        store_q: 数据提取进程将获取到的数据交给数据存储进程的通道
        """
    # 初始化队列4个队列
    url_q = Queue()
    result_q = Queue()
    conn_q = Queue()
    store_q = Queue()
    # 创建分布式管理器
    node = Controlmanager()
    manager = node.start_manager(url_q,result_q)
    # 创建URL管理进程、数据提取进程以及数据存储进程
    url_manager_proc = Process(target=node.url_manager_proc,args=(url_q,conn_q, 'https://baike.baidu.com/item/%E9%B3%97%E9%B1%BC/588',))
    result_solve_proc = Process(target=node.result_solve_proc,args=(result_q,conn_q,store_q,))
    store_proc = Process(target=node.store_proc,args=(store_q,))
    # 启动三个进程和分布式管理器
    url_manager_proc.start()
    result_solve_proc.start()
    store_proc.start()
    manager.get_server().serve_forever()


