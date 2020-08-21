package com.dask.sql.catalog.domain;

import java.util.ArrayList;
import java.util.Collection;

public class CatalogColumn implements Comparable {
	public CatalogColumn() {}

	public CatalogColumn(String name, CatalogColumnDataType type, int orderValue) {
		this.dataType = type;
		this.name = name;
		this.orderValue = orderValue;
	}

	private Long id;

	private String name;

	private CatalogColumnDataType dataType;

	private int orderValue;

	private CatalogTable table;

	public Long
	getId() {
		return id;
	}

	public void
	setId(Long id) {
		this.id = id;
	}

	public String
	getColumnName() {
		return this.name;
	}

	public void
	setColumnName(String name) {
		this.name = name;
	}

	public CatalogColumnDataType
	getColumnDataType() {
		return this.dataType;
	}

	public void
	setColumnDataType(CatalogColumnDataType dataType) {
		this.dataType = dataType;
	}

	public CatalogTable
	getTable() {
		return table;
	}

	public void
	setTable(CatalogTable newTable) {
		this.table = newTable;
	}

	public void
	setColumnDataType(String type) {
		this.dataType = CatalogColumnDataType.fromString(type);
	}

	public int
	getOrderValue() {
		return orderValue;
	}

	public void
	setOrderValue(int orderValue) {
		this.orderValue = orderValue;
	}

	public int
	compareTo(Object o) {
		Integer self = new Integer(this.orderValue);
		Integer other = new Integer(((CatalogColumn) o).getOrderValue());
		return self.compareTo(other);
	}
}
