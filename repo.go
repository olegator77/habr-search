package main

// Import package
import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"unicode"

	"io/ioutil"

	"git.itv.restr.im/itv-backend/reindexer"
	_ "git.itv.restr.im/itv-backend/reindexer/bindings/builtin"
	// _ "git.itv.restr.im/itv-backend/reindexer/pprof"
)

type HabrComment struct {
	ID     int    `reindex:"id,,pk" json:"id"`
	PostID int    `reindex:"post_id,,dense" json:"post_id"`
	Text   string `reindex:"text,text"  json:"text"`
	User   string `json:"user"`
	Time   int64  `json:"time"`
	Likes  int    `json:"likes,omitempty"`
}

type HabrPost struct {
	ID        int      `reindex:"id,tree,pk" json:"id"`
	Time      int64    `reindex:"time,tree,dense"  json:"time"`
	Text      string   `reindex:"text,-"  json:"text"`
	Title     string   `reindex:"title,-"  json:"title"`
	User      string   `reindex:"user" json:"user"`
	Hubs      []string `reindex:"hubs" json:"hubs"`
	Tags      []string `reindex:"tags" json:"tags"`
	Likes     int      `reindex:"likes" json:"likes,omitempty"`
	Favorites int      `reindex:"favorites" json:"favorites,omitempty"`
	Views     int      `reindex:"views" json:"views"`
	HasImage  bool     `json:"has_image,omitempty"`

	Comments []*HabrComment `reindex:"comments,,joined" json:"comments,omitempty"`
	_        struct{}       `reindex:"title+text=search,text,composite;dense"`
}

type Repo struct {
	db *reindexer.Reindexer
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
	var output bytes.Buffer
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
			if term == 0 && len(input) >= 3 {
				// enable suffix search from 2 symbols
				output.WriteByte('*')
			}
			termLen = 0
			interm = true
		}

		if !unicode.IsDigit(r) && !unicode.IsLetter(r) && !strings.Contains("-+/", string(r)) && interm {
			switch {
			case termLen >= 3:
				// enable typos search from 3 symbols in term
				output.WriteString("~*")
			case termLen >= 2:
				// enable prefix from 2 symbol or on 2-nd+ term
				output.WriteString("*")
			}
			output.WriteByte(' ')
			interm = false
			term++
		}
		if interm {
			output.WriteRune(r)
			termLen++
		}
	}

	if termLen <= 2 && term == 1 {
		return "xxxxxxxxx"
	}

	return output.String()
}

func (r *Repo) SearchPosts(text string, offset, limit int) ([]*HabrPost, int, error) {

	query := repo.db.Query("posts").
		Match("search", textToReindexFullTextDSL("*^1,title^1.3", text)).
		ReqTotal()

	query.Functions("text = snippet(<b>,</b>,20,20, ...,... <br/>)")

	applyOffsetAndLimit(query, offset, limit)

	it := query.Exec()

	if err := it.Error(); err != nil {
		return nil, 0, err
	}

	items := make([]*HabrPost, 0, 10)
	for it.Next() {
		item := it.Object()
		items = append(items, item.(*HabrPost))
	}

	return items, it.TotalCount(), nil
}

func (r *Repo) GetPost(id int, withComments bool) (*HabrPost, error) {

	query := repo.db.Query("posts").
		WhereInt("id", reindexer.EQ, id).
		ReqTotal()

	if withComments {
		query.Join(repo.db.Query("comments"), "comments").On("id", reindexer.EQ, "post_id")
	}

	it := query.Exec()

	obj, err := it.FetchOne()

	if err != nil {
		return nil, err
	}

	return obj.(*HabrPost), nil
}

func (r *Repo) GetPosts(offset int, limit int, user string, startTime int, endTime int, withComments bool) ([]*HabrPost, int, error) {

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

func (r *Repo) SearchComments(text string, offset, limit int) ([]*HabrComment, int, error) {
	query := repo.db.Query("comments").
		ReqTotal().
		Match("text", textToReindexFullTextDSL("", text))

	query.Functions("text = snippet(<b>,</b>,20,20, ...,... <br/>)")

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

func (r *Repo) RestoreFromFiles(path string) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	for i, f := range files {
		jsonItem, err := ioutil.ReadFile(path + "/" + f.Name())
		if err != nil {
			panic(err)
		}
		post := HabrPost{}
		err = json.Unmarshal(jsonItem, &post)
		if err != nil {
			log.Printf("Error parse file %s: %s\n", f.Name(), err.Error())
		}

		for _, comment := range post.Comments {
			comment.PostID = post.ID
			err = r.db.Upsert("comments", comment)
			if err != nil {
				log.Printf("Error upsert comment %d from file %s: %s\n", comment.ID, f.Name(), err.Error())
			}
		}

		if (i != 0 && (i%1000) == 0) || i == len(files)-1 {
			fmt.Printf("processed %d files (from %d)\n", i+1, len(files))
		}

		post.Comments = post.Comments[:0]
		err = r.db.Upsert("posts", post)
		if err != nil {
			log.Printf("Error upsert post from file %s: %s\n", f.Name(), err.Error())
		}

	}

}

func (r *Repo) Init() {

	r.db = reindexer.NewReindex("builtin:///tmp/reindex")
	r.db.SetLogger(logger)
	cfg := reindexer.DefaultFtFastConfig()
	cfg.MaxTyposInWord = 0
	cfg.LogLevel = reindexer.INFO

	err := r.db.OpenNamespace("comments", reindexer.DefaultNamespaceOptions(), HabrComment{})
	if err != nil {
		panic(err)
	}
	r.db.ConfigureIndex("comments", "text", cfg)
	it := r.db.Query("comments").Where("text", reindexer.EQ, "xx").Exec()
	if it.Error() != nil {
		panic(it.Error())
	}
	it.Close()

	err = r.db.OpenNamespace("posts", reindexer.DefaultNamespaceOptions(), HabrPost{})
	if err != nil {
		panic(err)
	}

	r.db.ConfigureIndex("posts", "search", cfg)
	it = r.db.Query("posts").Where("search", reindexer.EQ, "xx").Exec()
	if it.Error() != nil {
		panic(it.Error())
	}
	it.Close()

}

func (r *Repo) Done() {
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
