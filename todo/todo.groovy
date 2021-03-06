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
import groovy.io.FileType

writers = [:]
date = new Date().format('yyyy-MM-dd')
jsonSlurper = new JsonSlurper()

def httpGet(url) {
	zipfile = String.format('result%s.zip',date)
	def stream = new URL(url).openStream();
	byte[] arr = new byte[1024];
	def out = new FileOutputStream(zipfile);
	def len
	while((len=stream.read(arr))!=-1){
		out.write(arr,0,len)
	}
	out.close()
	return zipfile;
}

def getData() {
	print('\nstart to get undeal data....')
	def resultList = [];
	def dealList = [];

	def dir = new File('.')
	dir.traverse(type:FileType.FILES,
	    nameFilter:~/.[T_]*DealedTodo[_A-Za-z0-9-]*\.json/) {

	    it.eachLine("utf8") {
			if(it != null &&  it !='') {
				def o = jsonSlurper.parseText(it);
				dealList << (o.sourceId + ',' + o.appId + ',' + o.acceptId);
			}
		}
	}
	
	dir.traverse(type:FileType.FILES,
	    nameFilter:~/.[T_]*UndealTodo[_A-Za-z0-9-]*\.json/) {

		it.eachLine("utf8") {
			if(it != null &&  it !='') {
				def o = jsonSlurper.parseText(it);
				def key = o.sourceId + ',' + o.appId + ',' + o.acceptId;
				if(!dealList.contains(key)) {
					resultList << o;
				}
			}
		}
	}

	print('\n get undeal data end')
	if(resultList.size == 0) {
		print('\n has no data')
	}
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

	if (!source.isDirectory()) {  
   		print 'is not a directory'
   		return;
	} 

	def writers=[:]
	writers['T_DealedTodo'] =  new File(String.format('T_DealedTodo_%s.json', date))
	writers['T_UndealTodo'] =  new File(String.format('T_UndealTodo_%s.json', date))
	delete(writers['T_DealedTodo'])
	delete(writers['T_UndealTodo'])

    print 'start to filter...\n'
	source.eachFileRecurse { it ->  
		//if(it.name.contains(date)) {
		print 'find a file: ' + it.name + '\n'
		it.eachLine("utf8") {
			def o = jsonSlurper.parseText(it);
			writers[o.table].append('\n'+ new JsonBuilder(o),'utf-8');
		}
			
		//}
    }  
    print '\nfilter success'
}

def unzip(File dir){
	/*def zf = new java.util.zip.ZipFile(file)
	zf.entries().findAll { !it.directory }.each {
		println it.name
	}*/
	//上面的解压缩失败，尝试使用cmd的方式
	def output = date + '/'

	dir.traverse(type:FileType.FILES, nameFilter:~/.*\.zip/) {
		//print String.format('cmd /c 7z.exe x %s -y -aos -o%s',it,output).execute(null, new File("F:\\todomonitor")).text
		print String.format('cmd /c 7z.exe x %s -y -aos -o%s',it,output).execute().text
	}
	return output
}

df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
df.setTimeZone(TimeZone.getTimeZone("UTC"));

def prettyResult(list){
	log(list)
	resultMap = [:]
	list.each {
		/*def hour = df.parse(it.sendTime).getHours();
		if(resultMap.get(hour) == null) {
			def v = []
			resultMap[hour] = v;
		}
		resultMap.get(hour) << it*/
		
		def appId = it.appId;
		if(resultMap.get(appId) == null) {
			def v = []
			resultMap[appId] = v;
		}
		resultMap.get(appId) << it
		
	}

	//按小时打印结果
	def detail = '\n'
	resultMap.each {k,v ->
		//new File('./result.txt').append('\n'+ new JsonBuilder(k + ','+v),'utf-8')
		detail +='id为' + k + '的应用:\n';

		def size = 0;

		v.each {
			detail += 'sourceId=' +  it.sourceId  + ',acceptId=' + it. acceptId + '，创建时间' + it.sendTime +'\n'
			size++;
		}
		detail += ' 共' + size + '笔\n'
	}

	detail = '截止' + date + '日统计时，系统有' + list.size() + '笔的代办没有完成(by appId统计):\n' + detail

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

def log(list) {
	def result = new File('./result.txt')
	delete(result)
	list.each {
		result.append('\n'+ new JsonBuilder(it),'utf-8');
	}
}

def delete(File f){
	if(f.exists()) {
		f.delete();
	}
}

def zipfile = httpGet('http://120.131.8.136:8109/todotongji/'+ date + '_result.json.gz')
def unzipDir = unzip(new File('.'))
filter(new File(unzipDir))

def resultList = getData()
if(resultList.size() > 0) {
	send(prettyResult(resultList))
}