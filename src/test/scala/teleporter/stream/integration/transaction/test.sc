def unApply(key: String, template: String): Map[String, String] = {
  template.split("/").zip(key.split("/")).filter(_._1.startsWith(":"))
    .map { case (v1, v2) ⇒ (v1.tail, v2) }.toMap
}

unApply("/abc/ddd/eee", "/abc/:task/:aa")