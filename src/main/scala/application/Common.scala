package application

import java.io.File

import factories.Resources

/**
  *
  */
object Common {

  def checkWorkFolder(path: String): Boolean = {
    val file_with_both_data = new File(path + Resources.getBothFilename)
    val file_with_diff_data = new File(path + Resources.getDiffFilename)
    file_with_both_data.exists() && file_with_diff_data.exists()
  }
}
