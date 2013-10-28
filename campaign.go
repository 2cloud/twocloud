package twocloud

import (
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"secondbit.org/pan"
	"strings"
	"time"
)

var CampaignTableCreateStatement = `CREATE TABLE campaigns (
	id varchar primary key,
	title varchar NOT NULL,
	description varchar NOT NULL,
	goal bigint default 0,
	amount bigint default 0,
	auxilliary bool default false,
	starts timestamp default CURRENT_TIMESTAMP,
	ends timestamp);`

var MAXINT = 2147483647

const (
	CampaignCreatedTopic = "campaigns.created"
	CampaignUpdatedTopic = "campaigns.updated"
	CampaignDeletedTopic = "campaigns.deleted"
)

type Campaign struct {
	ID          ID        `json:"id,omitempty"`
	Title       string    `json:"title,omitempty"`
	Description string    `json:"description,omitempty"`
	Goal        int       `json:"goal,omitempty"`
	Amount      int       `json:"amount,omitempty"`
	Auxilliary  bool      `json:"auxilliary,omitempty"`
	Starts      time.Time `json:"starts,omitempty"`
	Ends        time.Time `json:"ends,omitempty"`
}

func (campaign *Campaign) fromRow(row ScannableRow) error {
	var idStr string
	var ends pq.NullTime
	err := row.Scan(&idStr, &campaign.Title, &campaign.Description, &campaign.Goal, &campaign.Amount, &campaign.Auxilliary, &campaign.Starts, &ends)
	if err != nil {
		return err
	}
	id, err := IDFromString(idStr)
	if err != nil {
		return err
	}
	campaign.ID = id
	if ends.Valid {
		campaign.Ends = ends.Time
	}
	return nil
}

var CampaignNotFoundError = errors.New("Campaign not found.")
var CampaignEmptyTitleError = errors.New("Campaign title empty")
var CampaignEmptyDescriptionError = errors.New("Campaign description empty")
var CampaignNegativeGoalError = errors.New("Campaign goal negative")

func (p *Persister) GetCampaigns(current, aux *bool, before, after ID, count int, admin bool) ([]Campaign, error) {
	campaigns := []Campaign{}
	var rows *sql.Rows
	var err error
	query := pan.New()
	query.SQL = "SELECT * FROM campaigns"
	if current != nil {
		query.IncludeWhere()
		if *current {
			query.Include("starts < ? and ends > ?", time.Now(), time.Now())
		} else {
			query.Include("ends < ?", time.Now())
		}
	}
	if aux != nil {
		query.IncludeWhere()
		query.Include("auxilliary=?", *aux)
	}
	if !before.IsZero() {
		query.IncludeWhere()
		query.Include("id < ?", before.String())
	}
	if !after.IsZero() {
		query.IncludeWhere()
		query.Include("id > ?", after.String())
	}
	if admin && current != nil {
		query.IncludeWhere()
		query.Include("starts < ?", time.Now())
	}
	query.FlushExpressions(" and ")
	query.IncludeOrder("starts")
	query.IncludeLimit(count)
	rows, err = p.Database.Query(query.Generate(" "), query.Args...)
	if err != nil {
		return []Campaign{}, err
	}
	for rows.Next() {
		var campaign Campaign
		err = campaign.fromRow(rows)
		if err != nil {
			return []Campaign{}, err
		}
		campaigns = append(campaigns, campaign)
	}
	err = rows.Err()
	return campaigns, err
}

func (p *Persister) GetCampaign(id ID, admin bool) (Campaign, error) {
	var campaign Campaign
	query := pan.New()
	query.SQL = "SELECT * FROM campaigns"
	query.IncludeWhere()
	query.Include("id=?", id.String())
	if !admin {
		query.Include("starts <= ?", time.Now())
	}
	row := p.Database.QueryRow(query.Generate(" and "), query.Args...)
	err := campaign.fromRow(row)
	if err == sql.ErrNoRows {
		err = CampaignNotFoundError
	}
	return campaign, err
}

func (p *Persister) AddCampaign(title, description string, goal int, aux bool, starts, ends time.Time) (Campaign, error) {
	id, err := p.GetID()
	if err != nil {
		return Campaign{}, err
	}
	title = strings.TrimSpace(title)
	if title == "" {
		return Campaign{}, CampaignEmptyTitleError
	}
	description = strings.TrimSpace(description)
	if description == "" {
		return Campaign{}, CampaignEmptyDescriptionError
	}
	if goal < 0 {
		return Campaign{}, CampaignNegativeGoalError
	}
	campaign := Campaign{
		ID:          id,
		Title:       title,
		Description: description,
		Goal:        goal,
		Auxilliary:  aux,
		Starts:      starts,
		Ends:        ends,
	}
	var startsPtr, endsPtr *time.Time
	if !starts.IsZero() {
		startsPtr = &starts
	}
	if !ends.IsZero() {
		endsPtr = &ends
	}
	query := pan.New()
	query.SQL = "INSERT INTO campaigns VALUES("
	query.Include("?", campaign.ID.String())
	query.Include("?", campaign.Title)
	query.Include("?", campaign.Description)
	query.Include("?", campaign.Goal)
	query.Include("?", 0)
	query.Include("?", campaign.Auxilliary)
	query.Include("?", startsPtr)
	query.Include("?", endsPtr)
	query.FlushExpressions(", ")
	query.SQL += ")"
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(CampaignCreatedTopic, nil, nil, &campaign.ID)
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return campaign, err
}

func (p *Persister) UpdateCampaign(campaign *Campaign, title, description *string, goal *int, aux *bool, starts, ends *time.Time) error {
	query := pan.New()
	query.SQL = "UPDATE campaigns SET "
	if title != nil {
		campaign.Title = strings.TrimSpace(*title)
		query.Include("title=?", campaign.Title)
	}
	if description != nil {
		campaign.Description = strings.TrimSpace(*description)
		query.Include("description=?", campaign.Description)
	}
	if goal != nil {
		campaign.Goal = *goal
		query.Include("goal=?", campaign.Goal)
	}
	if aux != nil {
		campaign.Auxilliary = *aux
		query.Include("auxilliary=?", campaign.Auxilliary)
	}
	if starts != nil {
		campaign.Starts = *starts
		query.Include("starts=?", campaign.Starts)
	}
	if ends != nil {
		campaign.Ends = *ends
		query.Include("ends=?", campaign.Ends)
	}
	query.FlushExpressions(", ")
	query.IncludeWhere()
	query.Include("id=?", campaign.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(CampaignUpdatedTopic, nil, nil, &campaign.ID)
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return err
}

func (p *Persister) UpdateCampaignAmount(campaign Campaign) (Campaign, error) {
	payments, err := p.GetPayments(ID(0), ID(0), MAXINT, []string{}, nil, &campaign.ID, nil) // get all payments in the campaign
	if err != nil {
		return campaign, nil
	}
	var sum int
	for _, payment := range payments {
		sum += payment.Amount
	}
	campaign.Amount = sum
	query := pan.New()
	query.SQL = "UPDATE campaigns SET"
	query.Include("amount=?", campaign.Amount)
	query.IncludeWhere()
	query.Include("id=?", campaign.ID.String())
	_, err = p.Database.Exec(query.Generate(" "), query.Args...)
	if err == nil {
		_, nsqErr := p.Publish(CampaignUpdatedTopic, nil, nil, &campaign.ID)
		if nsqErr != nil {
			p.Log.Error(nsqErr.Error())
		}
	}
	return campaign, err
}

func (p *Persister) DeleteCampaign(campaign Campaign) error {
	query := pan.New()
	query.SQL = "DELETE FROM campaigns"
	query.IncludeWhere()
	query.Include("id=?", campaign.ID.String())
	_, err := p.Database.Exec(query.Generate(" "), query.Args...)
	if err != nil {
		return err
	}
	_, nsqErr := p.Publish(CampaignDeletedTopic, nil, nil, &campaign.ID)
	if nsqErr != nil {
		p.Log.Error(nsqErr.Error())
	}
	// TODO: Should we cascade deletion to payments?
	return nil
}
