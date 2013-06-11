package twocloud

import (
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"strconv"
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

func (p *Persister) GetCampaigns(current, aux *bool, before, after ID, count int) ([]Campaign, error) {
	campaigns := []Campaign{}
	var rows *sql.Rows
	var err error
	if current != nil && aux != nil {
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE starts < $1 and ends > $1 and auxilliary=$2 and id < $3 and id > $4 ORDER BY starts LIMIT $5", time.Now(), *aux, before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns starts < $1 and ends > $1 and auxilliary=$2 and id < $3 ORDER BY starts LIMIT $4", time.Now(), *aux, before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns starts < $1 and ends > $1 and auxilliary=$2 and id > $3 ORDER BY starts LIMIT $4", time.Now(), *aux, after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM campaigns starts < $1 and ends > $1 and auxilliary=$2 ORDER BY starts LIMIT $3", time.Now(), *aux, count)
		}
	} else if current != nil && aux == nil {
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE starts < $1 and ends > $1 and id < $2 and id > $3 ORDER BY starts LIMIT $4", time.Now(), before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE starts < $1 and ends > $1 and id < $2 ORDER BY starts LIMIT $3", time.Now(), before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE starts < $1 and ends > $1 and id > $2 ORDER BY starts LIMIT $3", time.Now(), after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE starts < $1 and ends > $1 ORDER BY starts LIMIT $2", time.Now(), count)
		}
	} else if current == nil && aux != nil {
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE auxilliary=$1 and id < $2 and id > $3 ORDER BY starts LIMIT $4", *aux, before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE auxilliary=$1 and id < $2 ORDER BY starts LIMIT $3", *aux, before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE auxilliary=$1 and id > $2 ORDER BY starts LIMIT $3", *aux, after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE auxilliary=$1 ORDER BY starts LIMIT $2", *aux, count)
		}
	} else {
		if !before.IsZero() && !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE id < $1 and id > $2 ORDER BY starts LIMIT $3", before.String(), after.String(), count)
		} else if !before.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE id < $1 ORDER BY starts LIMIT $2", before.String(), count)
		} else if !after.IsZero() {
			rows, err = p.Database.Query("SELECT * FROM campaigns WHERE id > $1 ORDER BY starts LIMIT $2", after.String(), count)
		} else {
			rows, err = p.Database.Query("SELECT * FROM campaigns ORDER BY starts LIMIT $2", count)
		}
	}
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

func (p *Persister) GetCampaign(id ID) (Campaign, error) {
	var campaign Campaign
	row := p.Database.QueryRow("SELECT * FROM campaigns WEHRE id=$1", id.String())
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
	description = strings.TrimSpace(description)
	campaign := Campaign{
		ID:          id,
		Title:       title,
		Description: description,
		Goal:        goal,
		Auxilliary:  aux,
		Starts:      starts,
		Ends:        ends,
	}
	if !starts.IsZero() && !ends.IsZero() {
		stmt := `INSERT INTO campaigns VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9);`
		_, err = p.Database.Exec(stmt, campaign.ID.String(), campaign.Title, campaign.Description, campaign.Goal, 0, nil, campaign.Auxilliary, campaign.Starts, campaign.Ends, nil)
	} else if !starts.IsZero() {
		stmt := `INSERT INTO campaigns VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9);`
		_, err = p.Database.Exec(stmt, campaign.ID.String(), campaign.Title, campaign.Description, campaign.Goal, 0, nil, campaign.Auxilliary, campaign.Starts, nil, nil)
	} else if !ends.IsZero() {
		stmt := `INSERT INTO campaigns VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9);`
		_, err = p.Database.Exec(stmt, campaign.ID.String(), campaign.Title, campaign.Description, campaign.Goal, 0, nil, campaign.Auxilliary, nil, campaign.Ends, nil)
	} else {
		stmt := `INSERT INTO campaigns VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9);`
		_, err = p.Database.Exec(stmt, campaign.ID.String(), campaign.Title, campaign.Description, campaign.Goal, 0, nil, campaign.Auxilliary, nil, nil, nil)
	}
	return campaign, err
}

func (p *Persister) UpdateCampaign(campaign *Campaign, title, description *string, goal *int, aux *bool, starts, ends *time.Time) error {
	changedKeys := []string{}
	changedValues := []interface{}{}
	if title != nil {
		campaign.Title = strings.TrimSpace(*title)
		changedKeys = append(changedKeys, "title")
		changedValues = append(changedValues, campaign.Title)
	}
	if description != nil {
		campaign.Description = strings.TrimSpace(*description)
		changedKeys = append(changedKeys, "description")
		changedValues = append(changedValues, campaign.Description)
	}
	if goal != nil {
		campaign.Goal = *goal
		changedKeys = append(changedKeys, "goal")
		changedValues = append(changedValues, campaign.Goal)
	}
	if aux != nil {
		campaign.Auxilliary = *aux
		changedKeys = append(changedKeys, "auxilliary")
		changedValues = append(changedValues, campaign.Auxilliary)
	}
	if starts != nil {
		campaign.Starts = *starts
		changedKeys = append(changedKeys, "starts")
		changedValues = append(changedValues, campaign.Starts)
	}
	if ends != nil {
		campaign.Ends = *ends
		changedKeys = append(changedKeys, "ends")
		changedValues = append(changedValues, campaign.Ends)
	}
	stmt := `UPDATE campaigns SET`
	for index, value := range changedKeys {
		stmt += " " + value + "=$" + strconv.Itoa(index+1)
		if index+1 < len(changedKeys) {
			stmt += ","
		}
	}
	stmt += ` WHERE id=$` + strconv.Itoa(len(changedKeys)+1)
	changedValues = append(changedValues, campaign.ID)
	_, err := p.Database.Exec(stmt, changedValues...)
	return err
}

func (p *Persister) UpdateCampaignAmount(campaign Campaign) (Campaign, error) {
	// TODO: get payments by campaign ID
	// TODO: sum payments
	// TODO: save sum to campaign.Amount
	return campaign, nil
}

func (p *Persister) DeleteCampaign(campaign Campaign) error {
	stmt := `DELETE FROM campaigns WHERE id=$1;`
	_, err := p.Database.Exec(stmt, campaign.ID.String())
	if err != nil {
		return err
	}
	// TODO: Should we cascade deletion to payments?
	return nil
}
