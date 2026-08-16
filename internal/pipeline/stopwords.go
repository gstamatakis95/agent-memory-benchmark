package pipeline

// stopwords is the standard Snowball English stopword list
// (https://snowballstem.org/algorithms/english/stop.txt). Words are matched
// after Normalize (lowercase), before stemming.
var stopwords = func() map[string]bool {
	list := []string{
		"i", "me", "my", "myself", "we", "our", "ours", "ourselves",
		"you", "your", "yours", "yourself", "yourselves",
		"he", "him", "his", "himself", "she", "her", "hers", "herself",
		"it", "its", "itself", "they", "them", "their", "theirs", "themselves",
		"what", "which", "who", "whom", "this", "that", "these", "those",
		"am", "is", "are", "was", "were", "be", "been", "being",
		"have", "has", "had", "having", "do", "does", "did", "doing",
		"would", "should", "could", "ought",
		"a", "an", "the", "and", "but", "if", "or", "because", "as",
		"until", "while", "of", "at", "by", "for", "with", "about",
		"against", "between", "into", "through", "during", "before",
		"after", "above", "below", "to", "from", "up", "down", "in",
		"out", "on", "off", "over", "under", "again", "further",
		"then", "once", "here", "there", "when", "where", "why", "how",
		"all", "any", "both", "each", "few", "more", "most", "other",
		"some", "such", "no", "nor", "not", "only", "own", "same",
		"so", "than", "too", "very",
		"can", "will", "just", "don", "now",
		// contraction fragments left behind after punctuation stripping
		// ("don't" -> "don", "t"; "i'm" -> "i", "m"; ...)
		"s", "t", "m", "d", "ll", "re", "ve", "ain", "aren", "couldn",
		"didn", "doesn", "hadn", "hasn", "haven", "isn", "mustn",
		"needn", "shan", "shouldn", "wasn", "weren", "won", "wouldn",
	}
	m := make(map[string]bool, len(list))
	for _, w := range list {
		m[w] = true
	}
	return m
}()
