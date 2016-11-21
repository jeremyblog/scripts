@Grapes ([
@Grab(group='org.codehaus.groovy.modules.http-builder',module='http-builder',version='0.7'),
@Grab(group='com.gmongo', module='gmongo', version='1.2'),
@GrabExclude(group='org.codehaus.groovy', module='groovy', version='2.4.1'),
@GrabExclude(group='org.codehaus.groovy', module='groovy-xml', version='2.4.1')
])
import groovyx.net.http.*
import groovy.io.FileType
import static groovyx.net.http.ContentType.*    
import static groovyx.net.http.Method.*  
import groovy.json.JsonSlurper
//import com.gmongo.GMongoClient
//import com.mongodb.MongoCredential
import com.gmongo.*
import com.mongodb.ServerAddress
import com.mongodb.DBCursor
import com.mongodb.BasicDBObject
import groovy.json.JsonBuilder

slurper = new JsonSlurper()

//def _solr_doc_temp = getDocMessage();

//def _solr_user_temp = addUserName('_solr_doc_temp')

addPerson('_solr_user_temp')

def getDocMessage(){
	//credentials = MongoCredential.createMongoCRCredential('admin', 'root', 'admin' as char[])
	//client = new GMongoClient(new ServerAddress("192.168.22.111",27017), [credentials])
	def mongo = new GMongo(new ServerAddress("192.168.22.111",27017))

	def db = mongo.getDB('snsDev')
	def files = db.getCollection("T_DocMessage")
	//print files.find().count()
	def cur = files.find("time": [$gt: new Date("2015/10/15 0:0:0")])//"2015-10-15T14:33:34.463Z"

	def output = new File('_solr_doc_temp')
	if(output.exists()){
		output.delete()
	} 

	//print cur.toString()
	while(cur.hasNext()){
		 def line = cur.next();
		 def json = new JsonBuilder()
		 json {
		 	id line.messageId
			sendTime  line.time
			fileId  line.docId
			sender  line.senderId
			fileSize  line.length
			groupId  line.threadId
			person  null
			filename  line.fileName
			fileExt  line.fileType
		}
		 output.append('\n' + json.toString(),'utf-8')
	}
	return '_solr_doc_temp'
}

def addUserName(input){
	def mongo = new GMongo(new ServerAddress("192.168.22.111",27017))

	def db = mongo.getDB('ossDev')
	def users = db.getCollection("T_User")
	def cache = [:];
	def i = new File(input)
	if(!i.exists()) {
		print 'can not find file '
		return
	}

	def output = new File('_solr_user_temp')
	if(output.exists()){
		output.delete()
	}

	//def json = new JsonBuilder()
	i.eachLine("utf8") {
		if(it) {
			def doc = slurper.parseText(it)
			if(cache.get(doc.sender)) {
				doc.senderName = cache.get(doc.sender)
			} else {
				def u = users.findOne(_id: doc.sender)
				def uname = u ? u.name : 'not found'
				doc.senderName = uname
				//print uname
				cache[doc.sender]=uname
			}
			output.append('\n' + doc.inspect(),"utf-8");
		}
	}

	return '_solr_user_temp'
}

def addPerson(input) {
	def mongo = new GMongo(new ServerAddress("192.168.22.155",27017))

	def db = mongo.getDB('xtdb')
	def users = db.getCollection("T_GroupUser")
	def cache = [:];

	def i = new File(input)
	if(!i.exists()) {
		print 'can not find file '
		return
	}

	def output = new File('_solr_person_temp')
	if(output.exists()){
		output.delete()
	}

	i.eachLine("utf8") {
		if(it) {
			def doc = Eval.me(it)

			if(cache.get(doc.sender)) {
				doc.person = cache.get(doc.groupId)
			} else {
				def persons = users.find(groupId: doc.groupId)
				def pList = [];
				persons.each  {
					pList << it.userId
				}

				doc.person = pList
				cache[doc.groupId]= pList
				output.append('\n' + doc.inspect(),"utf-8");
			}
		}
	}

}