package org.apache.druid.query.lookup
import java.util
import java.util.Optional

/**
 * This is a test class to get around including the druid-server dependency in this module
 */
class LookupReferencesManager extends LookupExtractorFactoryContainerProvider {
  override def getAllLookupNames: util.Set[String] = java.util.Collections.emptySet()

  override def get(lookupName: String): Optional[LookupExtractorFactoryContainer] = Optional.empty()
}
