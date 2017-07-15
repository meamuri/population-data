package helpers

import java.io.File

import factories.Resources

/**
  *
  */
object Common {
  def folderContainFiles(paths: Iterable[String]): Boolean = {
    if (paths.isEmpty)
      return false

    for(p <- paths) {
      if (!new File(p).exists())
        return false
    }

    true
  }
}