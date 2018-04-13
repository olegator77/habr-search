package main

// Import package
import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"io/ioutil"

	"git.itv.restr.im/itv-backend/reindexer"
	_ "git.itv.restr.im/itv-backend/reindexer/bindings/builtin"
	_ "git.itv.restr.im/itv-backend/reindexer/pprof"
)

type HabrComment struct {
	ID     int    `reindex:"id,,pk" json:"id"`
	PostID int    `reindex:"post_id" json:"post_id"`
	Text   string `reindex:"text,text"  json:"text"`
	User   string `json:"user"`
	Time   int64  `json:"time"`
}

type HabrPost struct {
	ID       int            `reindex:"id,tree,pk" json:"id"`
	Time     int64          `reindex:"time,tree"  json:"time"`
	Text     string         `reindex:"text,-"  json:"text"`
	Title    string         `reindex:"title,-"  json:"title"`
	User     string         `reindex:"user" json:"user"`
	Comments []*HabrComment `reindex:"comments,,joined" json:"comments,omitempty"`
	_        struct{}       `reindex:"title+text=search,text,composite"`
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

func (r *Repo) SearchPosts(text string, offset, limit int) ([]*HabrPost, int, error) {

	query := repo.db.Query("posts").
		Match("search", text).
		Functions("text = snippet(<b>,</b>,20,20,...,...\n)").
		ReqTotal()

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

	items := make([]*HabrPost, 0, 10)
	for it.Next() {
		item := it.Object()
		items = append(items, item.(*HabrPost))
	}

	return items, it.TotalCount(), nil
}

func (r *Repo) SearchComments(text string, offset, limit int) ([]*HabrComment, int, error) {

	query := repo.db.Query("comments").
		ReqTotal().
		Match("text", text).
		Functions("text = snippet(<b>,</b>,20,20,...,...\n)")

	applyOffsetAndLimit(query, offset, limit)

	it := query.Exec()
	defer it.Close()

	if err := it.Error(); err != nil {
		return nil, 0, err
	}

	items := make([]*HabrComment, 0, 10)
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
			fmt.Printf("err parse %s\n", err.Error())
		}

		for _, comment := range post.Comments {
			comment.PostID = post.ID
			err = r.db.Upsert("comments", comment, "id=serial()")
			if err != nil {
				fmt.Printf("err upsert %s\n", err.Error())
			}
		}

		if (i % 1000) == 0 {
			fmt.Printf("processed %d files\n", i)
		}

		post.Comments = post.Comments[:0]
		err = r.db.Upsert("posts", post)
		if err != nil {
			fmt.Printf("err upsert %s\n", err.Error())
		}

	}

}

func (r *Repo) Warmup() {
	// Build index
	it := r.db.Query("posts").Where("search", reindexer.EQ, "xx").Exec()
	if it.Error() != nil {
		panic(it.Error())
	}
	it.Close()
	it = r.db.Query("comments").Where("text", reindexer.EQ, "xx").Exec()
	if it.Error() != nil {
		panic(it.Error())
	}
	it.Close()
}

func (r *Repo) Init() {

	os.RemoveAll("/tmp/reindex")
	r.db = reindexer.NewReindex("builtin:///tmp/reindex")
	r.db.SetLogger(logger)

	err := r.db.OpenNamespace("posts", reindexer.DefaultNamespaceOptions(), HabrPost{})
	if err != nil {
		panic(err)
	}

	err = r.db.OpenNamespace("comments", reindexer.DefaultNamespaceOptions(), HabrComment{})
	if err != nil {
		panic(err)
	}

	cfg := reindexer.DefaultFtFastConfig()
	cfg.MaxTyposInWord = 0
	cfg.LogLevel = reindexer.INFO
	r.db.ConfigureIndex("posts", "search", cfg)
	r.db.ConfigureIndex("comments", "text", cfg)

}

type Logger struct {
}

func (l *Logger) Printf(level int, format string, msg ...interface{}) { log.Printf(format, msg...) }

var logger = &Logger{}
