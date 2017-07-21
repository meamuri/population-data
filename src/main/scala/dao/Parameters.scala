package dao

import factories.Resources

case class Parameters(path: String = Resources.getDataPath, year: Int = -1)
