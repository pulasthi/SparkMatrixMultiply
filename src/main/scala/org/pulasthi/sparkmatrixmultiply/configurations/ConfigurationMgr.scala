package org.pulasthi.sparkmatrixmultiply.configurations

/**
 * Created by pulasthiiu on 10/26/15.
 */;

object ConfigurationMgr{
  def LoadConfiguration(configurationFilePath: String): ConfigurationMgr = {
    return new ConfigurationMgr(configurationFilePath)
  }
}

class ConfigurationMgr (filePath: String) {
  var configurationFilePath: String = filePath;
  var damdsSection: DAMDSSection = new DAMDSSection(configurationFilePath)
}
