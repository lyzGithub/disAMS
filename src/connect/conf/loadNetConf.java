package connect.conf;

import java.io.File;   
import javax.xml.parsers.DocumentBuilder;   
import javax.xml.parsers.DocumentBuilderFactory;   
import org.w3c.dom.Document;   
import org.w3c.dom.NodeList;   
//read disHDFS address from conf/xml
public class loadNetConf {
	public loadNetConf() {
		
	}

	public  String readRemoteAddressByName(String name) {
		return  this.getInfoFromXml(name);
		
	}
	public String getRmiPort(String name){
		return this.getInfoFromXml(name);
	}
	private  String getInfoFromXml(String name) {

		try {
			File f = new File("conf/disAMS.xml");
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(f);
			NodeList nl = doc.getElementsByTagName("property");
			for (int i = 0; i < nl.getLength(); i++) {
				String nameItem = doc.getElementsByTagName("name").item(i).getFirstChild().getNodeValue();
				if(nameItem.equals(name)){
					String valueItem = doc.getElementsByTagName("value").item(i)
							.getFirstChild().getNodeValue();
					return valueItem;
				}
			}
			return null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}