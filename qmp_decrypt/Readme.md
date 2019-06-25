***js 逆向破解企名片


**配置环境：
	node.js
	webstrom
	python3.6
	

**参考资料：
	http://www.threetails.xyz/2019/05/10/%E5%88%9D%E6%8E%A2js%E9%80%86%E5%90%91/
	https://mp.weixin.qq.com/s/uoAnLYNrTsNn_YowWyXfyg
	十分感谢以上两位大佬的文章
	
	
**在参考资料里有全过程的详细讲解，本人也是一次操作js逆向，分享一下心得：
	js基础需要了解一下，可以去菜鸟教程学习或者廖雪峰老师的网站
	其次是在分析网页，寻找js加密关键段落的时候，要学会调试，多摸索，刚开始可能会一头懵
	
	
**因为网站略微有些改动，我的代码与参考资料中的略有不同
	调用完cxt.call()可以直接得到数据，不需要进行后续操作
	js代码略微有所改变
	
	
**遇到的bug
	在python中使用PyExecJS模块取运行js代码时，一直显示编码错误，原因是有一个程序在使用TextIOWrapper 类创建对象时默认使用了cp936的编码，
	也就是gbk编码，读取不了utf-8的字符，我们可以修改下 subprocess.py 文件的默认编码方式为utf-8即可，在614行将encoding='utf-8'
	
	
	
 