package tsstore

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

// PaginationConfig this is used to pass pagination information to the query. If the next token from a previous page is provided
// the results will be retrieved from that token onwards.
type PaginationConfig struct {
	MaxRows   int64
	NextToken string
}

type QueryManager struct {
	tsquery *timestreamquery.TimestreamQuery
}

func New(awscfg ...*aws.Config) *QueryManager {
	sess := session.Must(session.NewSession(awscfg...))

	return &QueryManager{
		tsquery: timestreamquery.New(sess),
	}
}

func (qm *QueryManager) Query(ctx context.Context, sql string, pagination PaginationConfig) (*timestreamquery.QueryOutput, error) {

	queryParams := &timestreamquery.QueryInput{
		QueryString: aws.String(sql),
	}

	if pagination.MaxRows != 0 {
		queryParams.MaxRows = aws.Int64(pagination.MaxRows)
	}

	if pagination.NextToken != "" {
		queryParams.NextToken = aws.String(pagination.NextToken)
	}

	return qm.tsquery.QueryWithContext(ctx, queryParams)
}
