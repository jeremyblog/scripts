@Grapes([
@Grab(group='org.codehaus.groovy.modules.http-builder',module='http-builder',version='0.7')

])
import groovyx.net.http.HTTPBuilder
import groovyx.net.http.*
import groovy.json.JsonSlurper
import static groovyx.net.http.Method.GET
import static groovyx.net.http.ContentType.TEXT
import groovy.json.JsonBuilder
import java.text.SimpleDateFormat

writers = [:]
date = new Date().format('yyyy-MM-dd')
jsonSlurper = new JsonSlurper()

def httpGet(url) {
	zipfile = String.format('./result%s.zip',date)
	def stream = new URL(url).openStream();
	byte[] arr = new byte[1024];
	def out = new FileOutputStream(zipfile);
	def len
	while((len=stream.read(arr))!=-1){
		out.write(arr,0,len)
	}
	return zipfile;
}

def initWriter(){
	writers['T_UndealTodo'] = new File(String.format('./T_UndealTodo%s.json',date));
	writers['T_DealedTodo'] = new File(String.format('./T_DealedTodo%s.json',date));
}

def getData(undeal, deal) {
	print('\nstart to get undeal data....')
	def resultList = [];
	def dealList = [];
	deal.eachLine("utf8") {
		if(it != null &&  it !='') {
			def o = jsonSlurper.parseText(it);
			dealList << (o.sourceId + ',' + o.appId + ',' + o.acceptId);
		}
	}

	undeal.eachLine("utf8") {
		if(it != null &&  it !='') {
			def o = jsonSlurper.parseText(it);
			def key = o.sourceId + ',' + o.appId + ',' + o.acceptId;
			if(!dealList.contains(key)) {
				resultList << o;
			}
		}
	}

	print('\n get undeal data end')
	return resultList;
}

//发送至代办
def send(content){
	def http = new HTTPBuilder('http://192.168.22.92/newtodo/');
	http.request(Method.POST ){
		uri.path = 'old/generatetodo'
		body = content

		requestContentType = ContentType.JSON
		response.success = {resp ->
			println "send done"
		}

		response.failure = { response ->
				print response.status
		}
	}
}

def filter(File source) {
	initWriter();

	if (!source.isDirectory()) {  
   		print 'is not a directory'
   		return;
	} 

	source.eachFileRecurse { it ->  
		if(it.name.contains(date)) {
        	print 'find a file: ' + it.name + '\n'
        	print 'start to filter...'
			it.eachLine("utf8") {
				def o = jsonSlurper.parseText(it);
				writers[o.table].append('\n'+ new JsonBuilder(o),'utf-8');
			}
			print '\nfilter success'
		}
    }  
}

def unzip(File file){
	/*def zf = new java.util.zip.ZipFile(file)
	zf.entries().findAll { !it.directory }.each {
		println it.name
	}*/
	//上面的解压缩失败，尝试使用cmd的方式
	def output = new File('./' + date)
	def cmd = '"C:\\Program Files\\7-Zip\\7z.exe" x '+ file + ' -y -aos -o' + output + '/'
	def proc =cmd.execute() 
	proc.waitFor()  // 用以等待外部进程调用结束 
	println proc.exitValue()
	return output
}

df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
df.setTimeZone(TimeZone.getTimeZone("UTC"));

def prettyResult(list){
	resultMap = [:]
	list.each {
		def hour = df.parse(it.sendTime).getHours();
		if(resultMap.get(hour) == null) {
			def v = []
			resultMap[hour] = v;
		}
		resultMap.get(hour) << it
		
	}

	//按小时打印结果
	def detail = '\n'
	resultMap.each {k,v ->
		//new File('./result.txt').append('\n'+ new JsonBuilder(k + ','+v),'utf-8')
		detail += k + '时详情如下:\n';

		def size = 0;
		v.each {
			detail += 'sourceId=' +  it.sourceId  + ',pubId='+ it.pubId + ',acceptId=' + it. acceptId + '\n'
			size++;
		}
		detail += '\n 共' + size + '笔\n'
	}

	detail =  date + '日，系统有如下的代办没有完成' + detail

	dataList = [
    	[status:'{"DO": 0,"READ": 0}', personId:'580099c500b0cf1c17fadc9e']
	]
	def builder 	= new JsonBuilder()  
	builder {
		params dataList.collect {data->
			  return {
	            //my_new_key ''
	            data.each {key, value ->
	                "$key" value
	            }
        }
		}
		content detail
		title '代办系统每日统计提醒'
		appId '-1'
		pubId 'XT-10001'
		senderId '580099c500b0cf1c17fadc9e'
		sourceId 'c2df4632-4157-492d-96c0-3a4a6908e37c'
	}
	return builder.toString()
}

def zipfile = httpGet('http://120.131.8.136:8109/todotongji/'+ date + '_result.json.gz')
def unzipDir = unzip(new File(zipfile))

filter(unzipDir)
//filter(new File('./' + date))
//initWriter();

def resultList = getData(writers['T_UndealTodo'],writers['T_DealedTodo'])
send(prettyResult(resultList))
