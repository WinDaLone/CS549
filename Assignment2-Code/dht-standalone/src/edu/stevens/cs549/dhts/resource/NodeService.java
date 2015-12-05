package edu.stevens.cs549.dhts.resource;

import java.awt.RenderingHints.Key;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.glassfish.jersey.spi.ResponseExecutorsProvider;

import edu.stevens.cs549.dhts.activity.DHT;
import edu.stevens.cs549.dhts.activity.DHTBase;
import edu.stevens.cs549.dhts.activity.DHTBase.Failed;
import edu.stevens.cs549.dhts.activity.DHTBase.Invalid;
import edu.stevens.cs549.dhts.activity.IDHTResource;
import edu.stevens.cs549.dhts.activity.NodeInfo;
import edu.stevens.cs549.dhts.main.Log;
import edu.stevens.cs549.dhts.main.Time;

/*
 * Additional resource logic.  The Web resource operations call
 * into wrapper operations here.  The main thing these operations do
 * is to call into the DHT service object, and wrap internal exceptions
 * as HTTP response codes (throwing WebApplicationException where necessary).
 * 
 * This should be merged into NodeResource, then that would be the only
 * place in the app where server-side is dependent on JAX-RS.
 * Client dependencies are in WebClient.
 * 
 * The activity (business) logic is in the dht object, which exposes
 * the IDHTResource interface to the Web service.
 */

public class NodeService {
	
	// TODO done: add the missing operations

	HttpHeaders headers;

	IDHTResource dht;
	
	private void info(String mesg) {
		Log.info(mesg);
	}

	public NodeService(HttpHeaders headers, UriInfo uri) {
		this.headers = headers;
		this.dht = new DHT(uri);
	}

	private static final String ns = "http://www.stevens.edu/cs549/dht";

	public static final QName nsNodeInfo = new QName(ns, "NodeInfo");

	public static JAXBElement<NodeInfo> nodeInfoRep(NodeInfo n) {
		return new JAXBElement<NodeInfo>(nsNodeInfo, NodeInfo.class, n);
	}

	private void advanceTime() {
		List<String> timestamps = headers.getRequestHeader(Time.TIME_STAMP);
		if (timestamps.size() != 1) {
			throw new WebApplicationException(Response.Status.BAD_REQUEST);
		}
		Time.advanceTime(Long.parseLong(timestamps.get(0)));
	}

	private Response response(NodeInfo n) {
		return Response.ok(nodeInfoRep(n)).header(Time.TIME_STAMP, Time.advanceTime()).build();
	}

	private Response response(TableRep t) {
		return Response.ok(t).header(Time.TIME_STAMP, Time.advanceTime()).build();
	}

	private Response response(TableRow r) {
		return Response.ok(tableRowRep(r)).header(Time.TIME_STAMP, Time.advanceTime()).build();
	}

	private Response responseNull() {
		return Response.notModified().header(Time.TIME_STAMP, Time.advanceTime()).build();
	}

	private Response response() {
		return Response.ok().header(Time.TIME_STAMP, Time.advanceTime()).build();
	}

	public Response getNodeInfo() {
		advanceTime();
		info("getNodeInfo()");
		return response(dht.getNodeInfo());
	}
	// Use tableRow
	public Response get(String k) {
		try {
			advanceTime();
			info("get(" + k + ")");
			return response(new TableRow(k, dht.get(k)));
		} catch (DHTBase.Invalid e) {
			info("error: invalid key");
			return responseNull();
		}
	}
	
	public Response add(String k, String v) {
		try {
			advanceTime();
			info("add(" + k + "," + v + ")");
			dht.add(k, v);
			return response();
		} catch (DHTBase.Invalid e) {
			info("error: invalid key");
			return responseNull();
		}
	}
	
	public Response delete(String k, String v) {
		try {
			advanceTime();
			info("delete(" + k + "," + v + ")");
			dht.delete(k, v);
			return response();
		} catch (DHTBase.Invalid e) {
			info("error invalid key");;
			return responseNull();
		}
	}
	
	public Response getPred() {
		advanceTime();
		info("getPred()");
		return response(dht.getPred());
	}
	
	public Response getSucc() {
		advanceTime();
		info("getSucc()");
		return response(dht.getSucc());
	}

	public Response notify(TableRep predDb) {
		advanceTime();
		info("notify()");
		TableRep db = dht.notify(predDb);
		if (db == null) {
			return responseNull();
		} else {
			return response(db);
		}
	}
	
	public Response closestPrecedingFinger(int id) {
		advanceTime();
		info("closestPrecedingFinger(" + id + ")");
		return response(dht.closestPrecedingFinger(id));
	}

	public static final QName nsTableRow = new QName(ns, "TableRow");

	public static JAXBElement<TableRow> tableRowRep(TableRow tr) {
		return new JAXBElement<TableRow>(nsTableRow, TableRow.class, tr);
	}

	public Response findSuccessor(int id) {
		try {
			advanceTime();
			info("findSuccessor()");
			return response(dht.findSuccessor(id));
		} catch (Failed e) {
			throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
		}
	}
	
}