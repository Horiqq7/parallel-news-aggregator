import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Article {
	@JsonProperty("uuid")
	public String uuid;

	@JsonProperty("title")
	public String title;

	@JsonProperty("author")
	public String author;

	@JsonProperty("text")
	public String text;

	@JsonProperty("published")
	public String published;

	@JsonProperty("language")
	public String language;

	@JsonProperty("categories")
	public List<String> categories;

	@JsonProperty("url")
	public String url;

	public Article() {}

	@Override
	public String toString() {
		return "Article{" + "uuid='" + uuid + '\'' + ", title='" + title + '\'' + '}';
	}
}