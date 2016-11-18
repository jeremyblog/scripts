@Grapes ([
@Grab(group='org.codehaus.groovy.modules.http-builder',module='http-builder',version='0.7'),
@Grab(group='com.gmongo', module='gmongo', version='1.2'),
@GrabExclude(group='org.codehaus.groovy', module='groovy', version='2.4.1'),
@GrabExclude(group='org.codehaus.groovy', module='groovy-xml', version='2.4.1')
])

import groovyx.net.http.*
import groovy.json.JsonSlurper
import groovy.io.FileType
import static groovyx.net.http.ContentType.*    
import static groovyx.net.http.Method.*  
//import com.gmongo.GMongoClient
//import com.mongodb.MongoCredential
import com.gmongo.*
import com.mongodb.ServerAddress
import com.mongodb.DBCursor
import com.mongodb.BasicDBObject

writers = [:]
sourceType = 'mongo'
targetType = 'solr'
sourceClear = false;
targetClear = false;
eq =  false;

initWirter(sourceType,sourceClear)
if(sourceClear) {
	getDataFromMongo(sourceType)
}
initWirter(targetType,targetClear)
if(targetClear) {
	getDataFromSolr(targetType)
}
compare(targetType, sourceType,eq)

//eq为true,找出source不存在于target的记录
//eq为false,找出source存在于target的记录
def compare(sourceType,targetType,eq) {
	def range = 0..1
	def prefix = eq ? "eq" : "nq"
	def output = new File(prefix + '_result.csv')
	if(output.exists()){
		output.delete()
	} 
	int i = 0;
	range.each {
		file = writers[targetType].get('_'+it)
		tList = []
		file.eachLine("utf8") {
			if(it) {
				tList << it.trim()
			}
		}

		file = writers[sourceType].get('_'+it)
		file.eachLine("utf8") {
			i ++
			def v = it.trim()
			if(eq) {
				if(v && tList.contains(v)) {
				//donothing
					output.append('\n' + v,'utf-8')
				} 
			} else if(v && !tList.contains(v)) {
				output.append('\n' + v,'utf-8')
			}
		}
	}
}


def getDataFromMongo(type){
	//credentials = MongoCredential.createMongoCRCredential('admin', 'root', 'admin' as char[])
	//client = new GMongoClient(new ServerAddress("192.168.22.111",27017), [credentials])
	def mongo = new GMongo(new ServerAddress("192.168.22.111",27017))

	def db = mongo.getDB('snsDev')
	def files = db.getCollection("T_DocMessage")
	//print files.find().count()
	def cur = files.find()
	while(cur.hasNext()){	
		def line = cur.next()
		def filename = line.docId
		def groupID = line.threadId	
		parse((filename+',' + groupID),type)
	}
}

def getDataFromSolr(type){
	try {
		def s = 1;
		int start = 0
		int rows = 1000
		while(s > 0 ) {
			result = httpGetNew(start,rows)
			parseList(result,type)
			s = result.size();
			start += s;
		}
	} catch (Exception e) {
		print(e)
	}
}

def parseList(result,type) {
	result.each {
		def filename = it
		//print it
		writers[type].get(getfile(filename.hashCode())).append('\n' + filename,'utf-8')
	}
}

def parse(fileName,type) {
	//print fileName
	//print it
	writers[type].get(getfile(fileName.hashCode())).append('\n' + fileName,'utf-8')
}

def initWirter(type,del){
	if(writers && writers[type] && writers[type].size() > 0) {	
		//donothing 
	} else {
		def result = [:]
		def range = 0..99

		def target = new File('data_' + type);
		if(del && target.exists()){
			target.deleteDir()
		} 
		if (!target.exists()) {
			target.mkdirs()
		}

		range.each {
			result['_' + it] = new File("data_" + type + "/_" + it)
		}

		writers[type] = result;
	}
}

def getfile(hashCode) {
	str = hashCode + ''
	if(str.length() < 3) {
		return '_' + str.toInteger();
	}
	return '_' + str.substring(str.length() -2,str.length()).toInteger();
}

def httpGetNew(int start, int rows) {
	def http = new HTTPBuilder('http://192.168.22.92')  

	http.request(GET, JSON) {req -> 
		uri.path='/solr/FileMessage/select' 
		uri.query=[q:'*',start:start,rows:rows,wt:'json',fl:'fileId,groupId',sort:'sendTime asc']
	 	//headers.'User-Agent' = "Mozilla/5.0 Firefox/3.0.4"  
	    headers.Accept = 'application/json'

		response.success = { response, json ->
			//if(response.statusLine == 200) {
			if(json.response.numFound == 0) {
				return []
			}
			else{
				result =[]
				json.response.docs.each {
					print it
					//it.each {
					def filename = it.fileId
					def groupId = it.groupId
					result << (filename+',' + groupId)
					//}
				}
				return result;
			}
			//} else {
			//	return []
			//}
		}
		response.failure = { response ->
			print response.status
			return []
		}//
	}
}
