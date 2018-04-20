package main

// Import package
import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
)

type ErrorResponce struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

type HabrPostView struct {
	*HabrPost
	Link  string `json:"link"`
	Image string `json:"image"`
}

type PostsResponce struct {
	Items      []HabrPostView `json:"items"`
	TotalCount int            `json:"total_count,omitempty"`
	Success    bool           `json:"success"`
}

type HabrCommentView struct {
	*HabrComment
	Link string `json:"link"`
}

type CommentsResponce struct {
	Items      []HabrCommentView `json:"items"`
	TotalCount int               `json:"total_count,omitempty"`
	Success    bool              `json:"success"`
}

func respError(ctx *fasthttp.RequestCtx, httpCode int, err error) {
	resp := ErrorResponce{
		Success: false,
		Error:   err.Error(),
	}
	ctx.SetStatusCode(httpCode)
	ret, _ := json.Marshal(resp)
	ctx.Write(ret)
}

func respJSON(ctx *fasthttp.RequestCtx, data interface{}) {
	ctx.SetStatusCode(200)
	ctx.SetContentType("application/json; charset=utf-8")
	ret, _ := json.Marshal(data)
	ctx.Write(ret)
}

func convertComments(in []*HabrComment) (out []HabrCommentView) {
	out = make([]HabrCommentView, 0, len(in))
	for _, comment := range in {
		cv := HabrCommentView{
			HabrComment: comment,
			Link:        fmt.Sprintf("https://habrahabr.ru/post/%d/#comment_%d", comment.PostID, comment.ID),
		}
		out = append(out, cv)
	}
	return out
}

func convertPosts(in []*HabrPost) (out []HabrPostView) {
	out = make([]HabrPostView, 0, len(in))
	for _, post := range in {
		pv := HabrPostView{
			HabrPost: post,
			Link:     fmt.Sprintf("https://habrahabr.ru/post/%d/", post.ID),
		}
		if post.HasImage {
			pv.Image = fmt.Sprintf("/images/%d.jpeg", post.ID)
		}

		out = append(out, pv)
	}
	return out
}

func SearchPosts(ctx *fasthttp.RequestCtx) {
	text := string(ctx.QueryArgs().Peek("query"))
	limit, _ := ctx.QueryArgs().GetUint("limit")
	offset, _ := ctx.QueryArgs().GetUint("offset")
	sortBy := string(ctx.QueryArgs().Peek("sort_by"))
	sortDesc, _ := ctx.QueryArgs().GetUint("sort_desc")
	items, total, err := repo.SearchPosts(text, offset, limit, sortBy, sortDesc > 0)

	if err != nil {
		respError(ctx, 502, err)
		return
	}

	resp := PostsResponce{
		Items:      convertPosts(items),
		TotalCount: total,
		Success:    true,
	}

	respJSON(ctx, resp)
}

func GetPostsHandler(ctx *fasthttp.RequestCtx) {
	user := string(ctx.QueryArgs().Peek("user"))
	limit, _ := ctx.QueryArgs().GetUint("limit")
	offset, _ := ctx.QueryArgs().GetUint("offset")
	startTime, _ := ctx.QueryArgs().GetUint("start_time")
	endTime, _ := ctx.QueryArgs().GetUint("end_time")
	withComments, _ := ctx.QueryArgs().GetUint("with_comments")

	items, total, err := repo.GetPosts(offset, limit, user, startTime, endTime, withComments > 0)

	if err != nil {
		respError(ctx, 502, err)
		return
	}
	resp := PostsResponce{
		Items:      convertPosts(items),
		TotalCount: total,
		Success:    true,
	}

	respJSON(ctx, resp)
}

func SearchComments(ctx *fasthttp.RequestCtx) {
	text := string(ctx.QueryArgs().Peek("query"))
	limit, _ := ctx.QueryArgs().GetUint("limit")
	offset, _ := ctx.QueryArgs().GetUint("offset")
	sortBy := string(ctx.QueryArgs().Peek("sort_by"))
	sortDesc, _ := ctx.QueryArgs().GetUint("sort_desc")

	items, total, err := repo.SearchComments(text, offset, limit, sortBy, sortDesc > 0)

	if err != nil {
		respError(ctx, 502, err)
		return
	}

	resp := CommentsResponce{
		Items:      convertComments(items),
		TotalCount: total,
		Success:    true,
	}

	respJSON(ctx, resp)
}

func SearchHandler(ctx *fasthttp.RequestCtx) {
	sortBy := string(ctx.QueryArgs().Peek("search_type"))
	switch sortBy {
	case "posts", "":
		SearchPosts(ctx)
	case "comments":
		SearchComments(ctx)
	default:
		respError(ctx, 401, fmt.Errorf("Invalid search_type. Valid values are: 'comments' or 'posts'"))
	}

}

func GetPostHandler(ctx *fasthttp.RequestCtx) {
	id, _ := strconv.Atoi(ctx.UserValue("id").(string))
	withComments, _ := ctx.QueryArgs().GetUint("with_comments")

	item, err := repo.GetPost(id, withComments > 0)

	if err != nil {
		respError(ctx, 502, err)
		return
	}

	respJSON(ctx, item)
}

func ConfigureHandler(ctx *fasthttp.RequestCtx) {
	ns := ctx.UserValue("ns").(string)
	var newCfg FTConfig
	err := json.Unmarshal(ctx.PostBody(), &newCfg)
	if err != nil {
		respError(ctx, 502, err)
		return
	}
	err = repo.SetFTConfig(ns, newCfg)
	if err != nil {
		respError(ctx, 502, err)
		return
	}
	ctx.WriteString("ok")
	return
}

func GetDocHandler(ctx *fasthttp.RequestCtx) {
	urlPath := string(ctx.Path())

	target := path.Join(*webRootPath, urlPath)

	f, err := os.Stat(target)
	if err != nil || f.IsDir() {
		target = path.Join(*webRootPath, "index.html")
	}

	log.Printf("%s", target)

	ctx.SendFile(target)

}

func HandlerWrapper(handler func(ctx *fasthttp.RequestCtx)) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {

		t := time.Now()
		handler(ctx)
		latency := time.Now().Sub(t)

		log.Printf("%s %s %s %d %d %v", ctx.RemoteIP().String(), string(ctx.Method()), string(ctx.RequestURI()), ctx.Response.StatusCode(), len(ctx.Response.Body()), latency)
	}
}

func StartHTTP(addr string) {
	router := fasthttprouter.New()
	router.GET("/api/search_posts", SearchHandler)
	router.GET("/api/search", SearchHandler)
	router.GET("/api/posts/:id", GetPostHandler)
	router.GET("/api/posts", GetPostsHandler)
	router.POST("/api/configure/:ns", ConfigureHandler)
	router.GET("/images/*filepath", GetDocHandler)
	router.GET("/static/*filepath", GetDocHandler)
	router.GET("/index.html", GetDocHandler)
	router.GET("/search", GetDocHandler)
	router.GET("/", GetDocHandler)
	log.Printf("Starting listen fasthttp on %s", addr)
	if err := fasthttp.ListenAndServe(addr, HandlerWrapper(router.Handler)); err != nil {
		panic(err)
	}
}
