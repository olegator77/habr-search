package main

// Import package
import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode"

	"io/ioutil"

	"git.itv.restr.im/itv-backend/reindexer"
	_ "git.itv.restr.im/itv-backend/reindexer/bindings/builtin"
	_ "git.itv.restr.im/itv-backend/reindexer/pprof"
)

type HabrComment struct {
	ID     int      `reindex:"id,,pk" json:"id"`
	PostID int      `reindex:"post_id,,dense" json:"post_id"`
	Text   string   `reindex:"text,-,dense"  json:"text"`
	User   string   `reindex:"user,-,dense" json:"user"`
	Time   int64    `reindex:"time,-,dense" json:"time"`
	Likes  int      `reindex:"likes,-,dense" json:"likes,omitempty"`
	_      struct{} `reindex:"text+user=search,text,composite"`
}

type HabrPost struct {
	ID        int      `reindex:"id,tree,pk" json:"id"`
	Time      int64    `reindex:"time,tree,dense"  json:"time"`
	Text      string   `reindex:"text,-"  json:"text"`
	Title     string   `reindex:"title,-"  json:"title"`
	User      string   `reindex:"user" json:"user"`
	Hubs      []string `reindex:"hubs" json:"hubs"`
	Tags      []string `reindex:"tags" json:"tags"`
	Likes     int      `reindex:"likes,-,dense" json:"likes,omitempty"`
	Favorites int      `reindex:"favorites,-,dense" json:"favorites,omitempty"`
	Views     int      `reindex:"views,-,dense" json:"views"`
	HasImage  bool     `json:"has_image,omitempty"`

	Comments []*HabrComment `reindex:"comments,,joined" json:"comments,omitempty"`
	_        struct{}       `reindex:"title+text+user=search,text,composite"`
}

type FTConfig struct {
	Bm25Boost      float64 `json:"bm25_boost"`
	Bm25Weight     float64 `json:"bm25_weight"`
	DistanceBoost  float64 `json:"distance_boost"`
	DistanceWeight float64 `json:"distance_weight"`
	TermLenBoost   float64 `json:"term_len_boost"`
	TermLenWeight  float64 `json:"term_len_weight"`
	MinRelevancy   float64 `json:"min_relevancy"`
	Fields         string  `json:"fields"`
}

type RepoConfig struct {
	PostsFt    FTConfig `json:"posts"`
	CommentsFt FTConfig `json:"comments"`
}

type Repo struct {
	db    *reindexer.Reindexer
	cfg   RepoConfig
	ready bool
}

func applyOffsetAndLimit(query *reindexer.Query, offset, limit int) {
	if limit != -1 {
		query.Limit(limit)
	} else {
		query.Limit(20)
	}

	if offset != -1 {
		query.Offset(offset)
	}
}

func textToReindexFullTextDSL(fields string, input string) string {
	var output, cur bytes.Buffer
	// Boost fields
	if len(fields) > 0 {
		output.WriteByte('@')
		output.WriteString(fields)
		output.WriteByte(' ')
	}

	interm := false
	term := 0
	termLen := 0

	// trim input spaces, and add trailing space
	input = strings.Trim(input, " ") + " "

	for _, r := range input {
		if (unicode.IsDigit(r) || unicode.IsLetter(r)) && !interm {
			cur.Reset()
			interm = true
			termLen = 0
		}

		if !unicode.IsDigit(r) && !unicode.IsLetter(r) && !strings.Contains("-+/", string(r)) && interm {

			if term > 0 {
				output.WriteByte('+')
			}
			switch {
			case termLen >= 3:
				// enable typos search from 3 symbols in term
				output.WriteString("*")
				output.Write(cur.Bytes())
				output.WriteString("~*")
			case termLen >= 2:
				// enable prefix from 2 symbol or on 2-nd+ term
				output.Write(cur.Bytes())
				output.WriteString("~*")
			default:
				output.Write(cur.Bytes())
			}
			output.WriteByte(' ')
			interm = false
			term++
			if term > 8 {
				break
			}
		}
		if interm {
			cur.WriteRune(r)
			termLen++
		}
	}

	if termLen <= 2 && term == 1 {
		return ""
	}

	return output.String()
}

func (r *Repo) SearchPosts(text string, offset, limit int, sortBy string, sortDesc bool) ([]*HabrPost, int, error) {

	if !r.ready {
		return nil, 0, fmt.Errorf("repo is not ready")
	}

	query := repo.db.Query("posts").
		Match("search", textToReindexFullTextDSL(r.cfg.PostsFt.Fields, text)).
		ReqTotal()

	query.Functions("text = snippet(<b>,</b>,30,30, ...,... <br/>)")

	if len(sortBy) != 0 {
		query.Sort(sortBy, sortDesc)
	}

	applyOffsetAndLimit(query, offset, limit)

	it := query.Exec()
	defer it.Close()

	if err := it.Error(); err != nil {
		return nil, 0, err
	}

	items := make([]*HabrPost, 0, it.Count())
	for it.Next() {
		item := it.Object()
		items = append(items, item.(*HabrPost))
	}

	return items, it.TotalCount(), nil
}

func (r *Repo) GetPost(id int, withComments bool) (*HabrPost, error) {
	if !r.ready {
		return nil, fmt.Errorf("repo is not ready")
	}

	query := repo.db.Query("posts").
		WhereInt("id", reindexer.EQ, id).
		ReqTotal()

	if withComments {
		query.Join(repo.db.Query("comments"), "comments").On("id", reindexer.EQ, "post_id")
	}

	it := query.Exec()
	defer it.Close()

	obj, err := it.FetchOne()

	if err != nil {
		return nil, err
	}

	return obj.(*HabrPost), nil
}

func (r *Repo) GetPosts(offset int, limit int, user string, startTime int, endTime int, withComments bool) ([]*HabrPost, int, error) {
	if !r.ready {
		return nil, 0, fmt.Errorf("repo is not ready")
	}

	query := repo.db.Query("posts").
		ReqTotal()

	applyOffsetAndLimit(query, offset, limit)

	if startTime != -1 {
		query.WhereInt("time", reindexer.GE, startTime)
	}

	if endTime != -1 {
		query.WhereInt("time", reindexer.LE, endTime)
	}

	if len(user) > 0 {
		query.WhereString("user", reindexer.EQ, user)
	}

	if withComments {
		query.Join(repo.db.Query("comments"), "comments").On("id", reindexer.EQ, "post_id")
	}

	query.Sort("time", false)

	it := query.Exec()
	defer it.Close()

	if err := it.Error(); err != nil {
		return nil, 0, err
	}

	items := make([]*HabrPost, 0, it.Count())
	for it.Next() {
		item := it.Object()
		items = append(items, item.(*HabrPost))
	}

	return items, it.TotalCount(), nil
}

func (r *Repo) SearchComments(text string, offset, limit int, sortBy string, sortDesc bool) ([]*HabrComment, int, error) {
	if !r.ready {
		return nil, 0, fmt.Errorf("repo is not ready")
	}

	query := repo.db.Query("comments").
		ReqTotal().
		Match("search", textToReindexFullTextDSL(r.cfg.CommentsFt.Fields, text))

	query.Functions("text = snippet(<b>,</b>,30,30, ...,... <br/>)")

	if len(sortBy) != 0 {
		query.Sort(sortBy, sortDesc)
	}

	applyOffsetAndLimit(query, offset, limit)

	it := query.Exec()
	defer it.Close()

	if err := it.Error(); err != nil {
		return nil, 0, err
	}

	items := make([]*HabrComment, 0, it.Count())
	for it.Next() {
		item := it.Object()
		items = append(items, item.(*HabrComment))
	}

	return items, it.TotalCount(), nil
}

func (r *Repo) updatePostFromFile(filePath string) {
	jsonItem, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Printf("Error read file %s: %s\n", filePath, err.Error())
	}
	post := HabrPost{}
	err = json.Unmarshal(jsonItem, &post)
	if err != nil {
		log.Printf("Error parse file %s: %s\n", filePath, err.Error())
	}

	for _, comment := range post.Comments {
		comment.PostID = post.ID
		err = r.db.Upsert("comments", comment)
		if err != nil {
			log.Printf("Error upsert comment %d from file %s: %s\n", comment.ID, filePath, err.Error())
		}
	}

	post.Comments = post.Comments[:0]
	err = r.db.Upsert("posts", post)
	if err != nil {
		log.Printf("Error upsert post from file %s: %s\n", filePath, err.Error())
	}

}

func (r *Repo) RestoreAllFromFiles(path string) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	for i, f := range files {
		r.updatePostFromFile(path + "/" + f.Name())
		if (i != 0 && (i%1000) == 0) || i == len(files)-1 {
			fmt.Printf("processed %d files (from %d)\n", i+1, len(files))
		}
	}
}

func (r *Repo) RestoreRangeFromFiles(path string, startID, finishID int) {

	cnt := 0
	for id := startID; id < finishID; id++ {
		fileName := fmt.Sprintf("%s/%d.json", path, id)
		if _, err := os.Stat(fileName); err == nil {
			r.updatePostFromFile(fileName)
			cnt++
		}
	}
	fmt.Printf("processed %d files\n", cnt+1)
}

func (r *Repo) setFTConfig(ns string, newCfg FTConfig) error {

	cfg := reindexer.DefaultFtFastConfig()
	cfg.MaxTyposInWord = 1
	cfg.LogLevel = reindexer.INFO
	cfg.Bm25Boost = newCfg.Bm25Boost
	cfg.Bm25Weight = newCfg.Bm25Weight
	cfg.DistanceBoost = newCfg.DistanceBoost
	cfg.DistanceWeight = newCfg.DistanceWeight
	cfg.MinRelevancy = newCfg.MinRelevancy

	err := r.db.ConfigureIndex(ns, "search", cfg)

	if err != nil {
		return err
	}

	switch ns {
	case "posts":
		r.cfg.PostsFt = newCfg
	case "comments":
		r.cfg.CommentsFt = newCfg
	default:
		return fmt.Errorf("Unknown namespace %s", ns)
	}
	return nil
}

func (r *Repo) SetFTConfig(ns string, newCfg FTConfig) error {
	err := r.setFTConfig(ns, newCfg)
	if err != nil {
		return err
	}
	data, err := json.Marshal(r.cfg)
	if err != nil {
		return err
	}
	return ioutil.WriteFile("repo.cfg", data, 0666)
}

func (r *Repo) Init() {

	if r.db == nil {
		r.db = reindexer.NewReindex("builtin:///var/lib/reindexer/habr")
		r.db.SetLogger(logger)
	}
	cfgFile, err := ioutil.ReadFile("repo.cfg")
	newCfg := RepoConfig{}

	if err != nil {
		err = json.Unmarshal(cfgFile, &newCfg)
	}

	if err != nil {

		newCfg.PostsFt = FTConfig{
			Bm25Boost:      0.1,
			Bm25Weight:     0.3,
			DistanceBoost:  2.0,
			DistanceWeight: 0.5,
			MinRelevancy:   0.2,
			Fields:         "*^0.4,user^1.0,title^1.6",
		}
		newCfg.CommentsFt = FTConfig{
			Bm25Boost:      0.1,
			Bm25Weight:     0.3,
			DistanceBoost:  2.0,
			DistanceWeight: 0.5,
			MinRelevancy:   0.2,
			Fields:         "",
		}
	}
	// cfg.StopWords = []string{"делать", "работать", "например", "получить", "данные", "стоит", "имеет", "компании", "случае", "код", "образом", "возможность", "работает", "свой", "т", "данных",
	// 	"сделать", "0", "позволяет", "помощью", "сразу", "4", "3", "6", "момент", "таким", "работы", "2", "использовать",
	// 	"с", "достаточно", "является", "часть", "10", "поэтому", "количество"}

	if err = r.db.OpenNamespace("comments", reindexer.DefaultNamespaceOptions(), HabrComment{}); err != nil {
		panic(err)
	}
	if err = r.setFTConfig("comments", newCfg.CommentsFt); err != nil {
		panic(err)
	}

	if err = r.db.OpenNamespace("posts", reindexer.DefaultNamespaceOptions(), HabrPost{}); err != nil {
		panic(err)
	}
	if err = r.setFTConfig("posts", newCfg.PostsFt); err != nil {
		panic(err)
	}
	repo.WarmUp()
}

func (r *Repo) WarmUp() {
	it := r.db.Query("comments").Where("search", reindexer.EQ, "").Exec()
	if it.Error() != nil {
		log.Print(it.Error().Error())
	}
	it.Close()
	it = r.db.Query("posts").Where("search", reindexer.EQ, "").Exec()
	if it.Error() != nil {
		log.Print(it.Error().Error())
	}
	r.ready = true
	it.Close()
}

func (r *Repo) Done() {
	r.ready = false
	r.db.CloseNamespace("posts")
	r.db.CloseNamespace("comments")
}

type Logger struct {
}

func (l *Logger) Printf(level int, format string, msg ...interface{}) {
	if level <= reindexer.TRACE {
		log.Printf(format, msg...)
	}
}

var logger = &Logger{}
