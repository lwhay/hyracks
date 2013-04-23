package edu.uci.ics.genomix.pregelix.util;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class GraphVertexOperation {
	
	/**
	 *  generate the valid data(byte[]) from BytesWritable
	 */
	public static byte[] generateValidDataFromBytesWritable(VKmerBytesWritable bw){
		byte[] wholeBytes = bw.getBytes();
		int validNum = bw.getLength();
		byte[] validBytes = new byte[validNum];
		for(int i = 0; i < validNum; i++)
			validBytes[i] = wholeBytes[i];
		return validBytes;
	}
	/**
	 * Single Vertex: in-degree = out-degree = 1
	 * @param vertexValue 
	 */
	public static boolean isPathVertex(byte value){
		if(GeneCode.inDegree(value) == 1 && GeneCode.outDegree(value) == 1)
			return true;
		return false;
	}
	/** 
	 * Head Vertex:  out-degree > 0, 
	 * @param vertexValue 
	 */
	public static boolean isHeadVertex(byte value){
		if(GeneCode.outDegree(value) > 0 && !isPathVertex(value))
			return true;
		return false;
	}
	/**
	 * Rear Vertex:  in-degree > 0, 
	 * @param vertexValue 
	 */
	public static boolean isRearVertex(byte value){
		if(GeneCode.inDegree(value) > 0 && !isPathVertex(value))
			return true;
		return false;
	}
	/**
	 * update right neighber based on next vertexId
	 */
	public static byte updateRightNeighberByVertexId(byte oldVertexValue, VKmerBytesWritable neighberVertex, int k){
		byte geneCode = neighberVertex.getGeneCodeAtPosition(k-1);
		
		byte newBit = GeneCode.getAdjBit(geneCode);
		return (byte) ((byte)(oldVertexValue & 0xF0) | (byte) (newBit & 0x0F));
	}
	/**
	 * update right neighber
	 */
	public static byte updateRightNeighber(byte oldVertexValue, byte newVertexValue){
		return (byte) ((byte)(oldVertexValue & 0xF0) | (byte) (newVertexValue & 0x0F));
	}

}