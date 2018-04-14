package main

// Import package
import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

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
			pv.Image = fmt.Sprintf("http://habr-demo.reindexer.org/images/%d.jpg", post.ID)
		}

		out = append(out, pv)
	}
	return out
}

func SearchPostsHandler(ctx *fasthttp.RequestCtx) {
	text := string(ctx.QueryArgs().Peek("query"))
	limit, _ := ctx.QueryArgs().GetUint("limit")
	offset, _ := ctx.QueryArgs().GetUint("offset")

	items, total, err := repo.SearchPosts(text, offset, limit)

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

func SearchCommentsHandler(ctx *fasthttp.RequestCtx) {
	text := string(ctx.QueryArgs().Peek("query"))
	limit, _ := ctx.QueryArgs().GetUint("limit")
	offset, _ := ctx.QueryArgs().GetUint("offset")

	items, total, err := repo.SearchComments(text, offset, limit)

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

func StartHTTP(addr string) {
	router := fasthttprouter.New()
	router.GET("/api/search_posts", SearchPostsHandler)
	router.GET("/api/search_comments", SearchCommentsHandler)
	router.GET("/api/posts/:id", GetPostHandler)
	router.GET("/api/posts", GetPostsHandler)
	log.Printf("Starting listen fasthttp on %s", addr)
	if err := fasthttp.ListenAndServe(addr, router.Handler); err != nil {
		panic(err)
	}
}
