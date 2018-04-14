package main

import (
	"bytes"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/nfnt/resize"
)

var months = map[string]int{"января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6, "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12}

func parseTime(htime string) (t time.Time, err error) {
	t = time.Now()
	timeParts := strings.Split(strings.Trim(htime, " "), " ")
	strDateTime := ""
	timeIdx := 2

	if len(timeParts[0]) == 8 && timeParts[0][2] == '.' {
		strDateTime = fmt.Sprintf("20%s-%s-%s", timeParts[0][6:8], timeParts[0][3:5], timeParts[0][0:2])
	} else if timeParts[0] == "сегодня" {
		strDateTime = fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
	} else if timeParts[0] == "вчера" {
		t = t.Add(-time.Hour * 24)
		strDateTime = fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
	} else if len(timeParts) > 2 {
		timeIdx++
		month, ok := months[timeParts[1]]
		if !ok {
			month = 1
		}
		year, err := strconv.Atoi(timeParts[2])
		if err == nil {
			timeIdx++
		} else {
			year = t.Year()
		}
		day, err := strconv.Atoi(timeParts[0])
		if err != nil {
			day = 1
		}
		strDateTime = fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	}

	if timeIdx < len(timeParts) {
		strDateTime += "T" + timeParts[timeIdx] + ":00+03:00"

		t, err = time.Parse(time.RFC3339, strDateTime)
	} else {
		err = fmt.Errorf("Can't parse time %s", htime)
	}

	return t, err

}

func downloadAndResizeImage(url string) (out []byte, err error) {
	resp, err := http.Get(url)

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s - Got %d status", url, resp.StatusCode)
	}

	ctype := resp.Header.Get("content-type")

	var img image.Image

	switch ctype {
	case "image/png":
		img, err = png.Decode(resp.Body)
	case "image/jpeg", "image/jpg":
		img, err = jpeg.Decode(resp.Body)
	case "image/gif":
		img, err = gif.Decode(resp.Body)
	default:
		return nil, fmt.Errorf("%s - Unknown image type %s", url, ctype)
	}

	if err != nil {
		return nil, err
	}

	// fmt.Printf("%s -> %s (%d,%d)\n", url, ctype, img.Bounds().Size().X, img.Bounds().Size().Y)

	img = resize.Thumbnail(100, 100, img, resize.Lanczos3)

	var buf bytes.Buffer
	jpeg.Encode(&buf, img, nil)

	return buf.Bytes(), nil
}

func DownloadPost(ID int) (*HabrPost, []byte, error) {

	url := fmt.Sprintf("https://habrahabr.ru/post/%d/", ID)

	doc, err := goquery.NewDocument(url)

	if err != nil {
		return nil, nil, err
	}

	var dpost, dcomments, dstats *goquery.Selection

	doc.Find("div").Each(func(i int, s *goquery.Selection) {
		if className, ok := s.Attr("class"); ok {
			if strings.Index(className, "post__wrapper") >= 0 {
				dpost = s
			}
			if strings.Index(className, "comments-section") >= 0 {
				dcomments = s
			}
			if strings.Index(className, "post-additionals") >= 0 {
				dstats = s
			}
		}
	})

	if dpost == nil {
		return nil, nil, fmt.Errorf("Data not found")
	}

	habrPost := &HabrPost{}
	var imgData []byte
	dpost.Find("div").Each(func(i int, s *goquery.Selection) {
		if className, ok := s.Attr("class"); ok {
			if strings.Index(className, "post__text") >= 0 {
				habrPost.Text = s.Text()
				img := s.Find("img").First()
				if img != nil {
					if srcURL, ok := img.Attr("src"); ok {
						imgData, err = downloadAndResizeImage(srcURL)
						if imgData != nil && err == nil {
							habrPost.HasImage = true
						}
					}
				}
			}
		}
	})

	dpost.Find("a").Each(func(i int, s *goquery.Selection) {
		if className, ok := s.Attr("class"); ok {
			if strings.Index(className, "inline-list__item-link hub-link") >= 0 {
				habrPost.Hubs = append(habrPost.Hubs, s.Text())
			}
			if strings.Index(className, "inline-list__item-link post__tag") >= 0 {
				habrPost.Tags = append(habrPost.Tags, s.Text())
			}
		}
	})

	dpost.Find("span").Each(func(i int, s *goquery.Selection) {
		if className, ok := s.Attr("class"); ok {
			if strings.Index(className, "post__title-text") >= 0 {
				habrPost.Title = s.Text()
			}
			if strings.Index(className, "post__time") >= 0 {
				t, err := parseTime(s.Text())
				if err != nil {
					fmt.Printf("Error parsing time %s", err.Error())
				}
				habrPost.Time = t.Unix()
			}
			if strings.Index(className, "user-info__nickname") >= 0 {
				habrPost.User = s.Text()
			}
		}
	})

	if dcomments != nil {
		dcomments.Find("div").Each(func(i int, s *goquery.Selection) {
			if className, ok := s.Attr("class"); ok {
				if strings.Index(className, "comment") >= 0 {
					comment := &HabrComment{}
					comment.ID = ID*1000 + len(habrPost.Comments)

					if commentIDStr, ok := s.Attr("id"); ok {
						commentIDStr = strings.TrimPrefix(commentIDStr, "comment_")
						if commentID, err := strconv.Atoi(commentIDStr); err == nil {
							comment.ID = commentID
						}
					}

					s.Find("div").Each(func(i int, s *goquery.Selection) {
						if className, ok := s.Attr("class"); ok {
							if strings.Index(className, "comment__message") >= 0 {
								comment.Text = s.Text()
							}
						}
					})
					s.Find("span").Each(func(i int, s *goquery.Selection) {
						if className, ok := s.Attr("class"); ok {
							if strings.Index(className, "user-info__nickname") >= 0 {
								comment.User = s.Text()
							}
							if strings.Index(className, "voting-wjt__counter") >= 0 {
								comment.Likes, _ = strconv.Atoi(s.Text())
							}
						}
					})
					s.Find("time").Each(func(i int, s *goquery.Selection) {
						if className, ok := s.Attr("class"); ok {
							if strings.Index(className, "comment__date-time") >= 0 {
								t, err := parseTime(s.Text())
								if err != nil {
									fmt.Printf("Error parsing time %s", err.Error())
								}
								comment.Time = t.Unix()
							}
						}
					})

					if len(comment.Text) > 0 {
						comment.PostID = ID
						habrPost.Comments = append(habrPost.Comments, comment)
					}
				}
			}
		})
	}
	if dstats != nil {
		dstats.Find("span").Each(func(i int, s *goquery.Selection) {
			if className, ok := s.Attr("class"); ok {
				if strings.Index(className, "voting-wjt__counter") >= 0 {
					habrPost.Likes, _ = strconv.Atoi(s.Text())
				}
				if strings.Index(className, "bookmark__counter") >= 0 {
					habrPost.Favorites, _ = strconv.Atoi(s.Text())
				}
				if strings.Index(className, "post-stats__views-count") >= 0 {
					viewsStr := strings.Replace(s.Text(), ",", ".", -1)
					mult := 1.0
					if kMultIdx := strings.Index(viewsStr, "k"); kMultIdx >= 0 {
						mult = 1000.0
						viewsStr = viewsStr[:kMultIdx]
					}

					views, _ := strconv.ParseFloat(viewsStr, 64)
					habrPost.Views = int(views * mult)
				}
			}
		})

	}

	habrPost.ID = ID

	return habrPost, imgData, nil
}
