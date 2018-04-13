package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

var months = map[string]int{"января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6, "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12}

func parseTime(htime string) (time.Time, error) {
	t := time.Now()
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
	} else if len(timeParts) > 1 {
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
	strDateTime += "T" + timeParts[timeIdx] + ":00+03:00"
	t, err := time.Parse(time.RFC3339, strDateTime)
	if err != nil {
		fmt.Printf("timeIdx=%d,%#v\n", timeIdx, timeParts)
	}

	return t, err

}

func DownloadPost(ID int) (*HabrPost, error) {

	// parseTime("10 июня 2012 в 10:00")

	url := fmt.Sprintf("https://habrahabr.ru/post/%d/", ID)

	doc, err := goquery.NewDocument(url)

	if err != nil {
		return nil, err
	}

	var dpost, dcomments *goquery.Selection

	doc.Find("div").Each(func(i int, s *goquery.Selection) {
		if className, ok := s.Attr("class"); ok {
			if strings.Index(className, "post__wrapper") >= 0 {
				dpost = s
			}
			if strings.Index(className, "comments-section") >= 0 {
				dcomments = s
			}
		}
	})

	if dpost == nil {
		return nil, fmt.Errorf("Data not found")
	}

	habrPost := &HabrPost{}
	dpost.Find("div").Each(func(i int, s *goquery.Selection) {
		if className, ok := s.Attr("class"); ok {
			if strings.Index(className, "post__text") >= 0 {
				habrPost.Text = s.Text()
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
	habrPost.ID = ID

	return habrPost, nil
}
