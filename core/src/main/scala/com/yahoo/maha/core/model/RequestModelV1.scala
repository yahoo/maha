package com.yahoo.maha.core.model

/**
  * Purpose:
  * RequestModel is a large, consolidated class which contains many other classes & objects all to generate internal
  * variables that are get & set on a one-shot FROM function.
  * The solution to this invented complexity is to separate out these companion objects into package-protected objects,
  * as well as moving variable validation into a validator.
  *
  * This must be done in a V1 class in order to avoid introducing a large RequestModel overhaul, allowing this process
  * to occur in a new place & override the behavior of the original incrementally.
  *
  * The problem that is solves is the egregious violations of OCP and SRP that occur throughout the requestModel,
  * causing any small edits made in this class to potentially balloon.
  */
class RequestModelV1 {

}
