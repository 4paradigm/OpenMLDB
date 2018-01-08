package com.google.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

public class ByteBufferNoCopy extends ByteString {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2389918830584219398L;

	public static ByteString wrap(ByteBuffer buffer) {
		return ByteString.wrap(buffer);
	}
	
	public static ByteString wrap(byte[] buffer) {
		return ByteString.wrap(buffer);
	}
	
	@Override
	public byte byteAt(int index) {
		
		return 0;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ByteString substring(int beginIndex, int endIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void copyToInternal(byte[] target, int sourceOffset, int targetOffset, int numberToCopy) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void copyTo(ByteBuffer target) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeTo(OutputStream out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	void writeToInternal(OutputStream out, int sourceOffset, int numberToWrite) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	void writeTo(ByteOutput byteOutput) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ByteBuffer asReadOnlyByteBuffer() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ByteBuffer> asReadOnlyByteBufferList() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String toStringInternal(Charset charset) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isValidUtf8() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected int partialIsValidUtf8(int state, int offset, int length) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public InputStream newInput() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CodedInputStream newCodedInput() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected int getTreeDepth() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected boolean isBalanced() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected int partialHash(int h, int offset, int length) {
		// TODO Auto-generated method stub
		return 0;
	}

	
}
