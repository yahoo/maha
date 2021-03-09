package com.yahoo.maha.core.helper.jdbc

import com.yahoo.maha.core.registry.RegistryBuilder


object SchemaGenerator {

  def generate(registryBuilder: RegistryBuilder, schemaDump: SchemaDump, dimConfig: DimConfig, factConfig: FactConfig
               , printDimensions: Boolean = false, printFacts: Boolean = false): Unit = {
    val dims = DimGenerator.buildPublicDimensions(schemaDump, dimConfig, printDimensions)
    val facts = FactGenerator.buildPublicFacts(schemaDump, factConfig, printFacts)
    dims.foreach(registryBuilder.register)
    facts.foreach(registryBuilder.register)
  }

}
