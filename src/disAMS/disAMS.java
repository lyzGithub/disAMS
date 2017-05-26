package disAMS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

import connect.conf.loadNetConf;

import disAMS.remoteAppMaster.remoteAPPMaster;

public class disAMS {

	private Map<String, remoteAPPMaster> rAMMap;
	private loadNetConf readXMLConf;
	public disAMS(){
		rAMMap = new HashMap<String, remoteAPPMaster>();
		readXMLConf = new loadNetConf();
	}
	
	public AllocateResponse GetRemoteAlloRequest(AllocateRequest request) throws Exception{
		
		
		//paste to AMS
		List<ResourceRequest> allAsk = request.getAskList();
	    List<ContainerId> allRelease = request.getReleaseList();
	    ResourceBlacklistRequest blacklistRequest =request.getResourceBlacklistRequest();
		
	    //this part distinguish the remote ask and release
	    List<ResourceRequest> localAsk = new ArrayList<ResourceRequest>();
	    List<ResourceRequest> remoteAsk = new ArrayList<ResourceRequest>();
	    //int askNum = allAsk.size();
	    for(Iterator it2 = allAsk.iterator();it2.hasNext();){
	    	ResourceRequest tmp = (ResourceRequest) it2.next();
	    	String typeItem[] = tmp.getResourceName().split(":");
	    	if(typeItem[0].contains("dis")){
	    		remoteAsk.add(tmp);
	    	}
	    	else {
	    		localAsk.add(tmp);
	    	}
        }
	    
	    
	    List<ContainerId> localRelease = new ArrayList<ContainerId>();
	    List<ContainerId> remoteRelease = new ArrayList<ContainerId>();
	    //?how to distinguish the local and remote
	    
	    AllocateRequest localAllocateRequest = AllocateRequest.newInstance(request.getResponseId(),
	    		(float) 0.1, localAsk, allRelease, blacklistRequest);
	    
	    AllocateRequest remoteAllocateRequest = AllocateRequest.newInstance(request.getResponseId(),
	    		(float) 0.1, remoteAsk, allRelease, blacklistRequest);
	    
	   
	    
	    
	    
	    
	    
		//remote resource_name: remotename:resource_name
		String[] remoteResourceName = request.getAskList().get(0).getResourceName().split(":");
		if(rAMMap.containsKey(remoteResourceName[0])){
			remoteAPPMaster rAM = rAMMap.get(remoteResourceName[0]);
			AllocateResponse response = rAM.GetAlloRequest(request);
			return response;
		}
		else{
			String remoteYarnAddress = readXMLConf.readRemoteAddressByName(remoteResourceName[0]);
			remoteAPPMaster rAM = newRemoteAM(remoteYarnAddress);
			rAMMap.put(remoteResourceName[0], rAM);
			AllocateResponse response = rAM.GetAlloRequest(request);
			return response;
			
		}
		
	}
	
	private remoteAPPMaster newRemoteAM(String yarnAddress) throws Exception{
		remoteAPPMaster rAM = new remoteAPPMaster(yarnAddress);
		return rAM;
		
	}

	public boolean unregisteAllRAM() throws YarnException, IOException {
		Iterator<?> iter = rAMMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			remoteAPPMaster rAM = (remoteAPPMaster)entry.getKey();
			rAM.unregisterAM();
		}
		return true;

	}
	
}
