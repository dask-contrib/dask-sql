package com.dask.sql.catalog.domain;

import org.hibernate.annotations.Cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.MapKey;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

// NOTE: not currenlty  mapped!
/**
 * This class is not currently being used.
 * @author felipe
 *
 */
@Entity
@Table(name = "blazing_catalog_schemas")
public class CatalogSchema {
	@Id @GeneratedValue @Column(name = "id") private Long id;

	@Column(name = "name", nullable = false) private String name;

	@OneToMany(fetch = FetchType.EAGER, mappedBy = "schema")
	@Cascade({org.hibernate.annotations.CascadeType.SAVE_UPDATE})
	@MapKey(name = "name")  // here this is the column name inside of CatalogColumn
	private Map<String, CatalogDatabase> schemaDatabases;


	public Long
	getId() {
		return id;
	}

	public void
	setId(Long id) {
		this.id = id;
	}

	public String
	getSchemaName() {
		return this.name;
	}

	public void
	getSchemaName(String name) {
		this.name = name;
	}

	public Set<CatalogDatabase>
	getDatabases() {
		Set<CatalogDatabase> tempDatabases = new LinkedHashSet<CatalogDatabase>();
		tempDatabases.addAll(this.schemaDatabases.values());
		return tempDatabases;
	}

	public Map<String, CatalogDatabase>
	getSchemaDatabases() {
		return this.schemaDatabases;
	}

	public void
	setDatabases(Map<String, CatalogDatabase> databases) {
		this.schemaDatabases = databases;
	}

	public CatalogDatabase
	getDatabaseByName(String databaseName) {
		return schemaDatabases.get(databaseName);
	}
}
