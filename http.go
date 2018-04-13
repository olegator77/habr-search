package main

// Import package
import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
)

type ErrorResponce struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

type PostsResponce struct {
	Items      []*HabrPost `json:"items"`
	TotalCount int         `json:"total_count,omitempty"`
	Success    bool        `json:"success"`
}

type CommentsResponce struct {
	Items      []*HabrComment `json:"items"`
	TotalCount int            `json:"total_count,omitempty"`
	Success    bool           `json:"success"`
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
		Items:      items,
		TotalCount: total,
		Success:    true,
	}

	ret, _ := json.Marshal(resp)
	ctx.Write(ret)
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
		Items:      items,
		TotalCount: total,
		Success:    true,
	}

	ret, _ := json.Marshal(resp)
	ctx.Write(ret)
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
		Items:      items,
		TotalCount: total,
		Success:    true,
	}

	ret, _ := json.Marshal(resp)
	ctx.Write(ret)
}

func GetPostHandler(ctx *fasthttp.RequestCtx) {
	id, _ := strconv.Atoi(ctx.UserValue("id").(string))
	withComments, _ := ctx.QueryArgs().GetUint("with_comments")

	item, err := repo.GetPost(id, withComments > 0)

	if err != nil {
		respError(ctx, 502, err)
		return
	}

	ret, _ := json.Marshal(item)
	ctx.Write(ret)
}

func StartHTTP() {
	router := fasthttprouter.New()
	router.GET("/search_posts", SearchPostsHandler)
	router.GET("/search_comments", SearchCommentsHandler)
	router.GET("/posts/:id", GetPostHandler)
	router.GET("/posts", GetPostsHandler)
	log.Printf("Starting listen fasthttp on 8881")
	if err := fasthttp.ListenAndServe(":8881", router.Handler); err != nil {
		panic(err)
	}
}
