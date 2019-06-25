***AJAX抓取图片

*知识点：
	
	Ajax动态页面抓取
	
	cchardet模块
	
	进程池
	
	本地存储
	
*Ajax:
	
	Ajax全称是Asynchronous JavaScript and XML(异步JavaScript 和 XML),
	
	不是一门编程语言，而是利用JavaScript在保证页面不被刷新的、页面链接
	
	不改变的情况下与服务器交换数据并更新部分网页的技术
	
*cchardet模块:
	cchardet是chardet的升级版，低层由C和C++实现，速度很快，用来判断网页的
	
	编码。
	
*大致思路：
	抓取Ajax网页需要利用Chrome，查看源码，在Network选项卡下勾选XHR，这样查看的
	
	链接都是我们所要请求的。查看每个链接，在header里找到所请求的URL，查看规律，
	
	一般只有一到两个变量。
	
	1. get_page_index():构造请求，获取初始网页源码
	
	2. parse_page_index():解析网页，得到每篇文章的详细title和url
	
	3. get_page_detail():从item中获取每篇文章的url，请求得到网页源码
	
	4. parse_page_detail():解析每篇文章，获取文章的title、图片URL、编号
	
	5. save_pic():存储到本地
	
