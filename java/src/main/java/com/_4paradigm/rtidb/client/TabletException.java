package com._4paradigm.rtidb.client;

public class TabletException extends Exception {

	private static final long serialVersionUID = 7871057496216257400L;
	private int errorCode = -1;
	
	public TabletException() {
		super();

	}

	public TabletException(String message, Throwable cause) {
		super(message, cause);
	}

	public TabletException(String message) {
		super(message);
	}

	public TabletException(Throwable cause) {
		super(cause);
	}

	public TabletException(int errorCode, String message) {
		super(message);
		this.errorCode = errorCode;
	}

	public int getCode() {
		return errorCode;
	}

}
