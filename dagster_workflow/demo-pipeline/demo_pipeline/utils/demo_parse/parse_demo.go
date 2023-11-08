package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	r3 "github.com/golang/geo/r3"
	dem "github.com/markus-wa/demoinfocs-golang/v4/pkg/demoinfocs"
	events "github.com/markus-wa/demoinfocs-golang/v4/pkg/demoinfocs/events"
)

func is_demo_cs2(f *os.File) bool {
	// TODO: In future split this out and point to 2 different parsers one for cs2 and one for csgo
	// currently just getting set up for csgo
	demo_type := make([]byte, 7)
	num_of_bytes, err := f.Read(demo_type)

	var is_cs2 bool

	if err != nil {
		log.Panic("failed to open demo file: ", err)
	}

	// If demo isn't for csgo close file and exit program
	if string(demo_type[:num_of_bytes]) == "HL2DEMO" {
		is_cs2 = false
	} else {
		is_cs2 = true
	}

	// Repointing reader to start for parser to actually use
	// Parser actually already checks first 8 bytes for this but it is called as part of creating the parser
	// so not 100% sure it's completely suited.

	f.Seek(0, io.SeekStart)

	return is_cs2

}

func main() {

	// Should only be passed two args which is path to demo_file, assumes that outfile has '/' at the end
	demo_path := os.Args[1]   //"/home/kelanmazey/csgo_demo_pipeline/dagster_workflow/demo-pipeline/demos/82330/bravado-vs-goliath-m1-anubis.dem"
	outpath_arg := os.Args[2] //"./test.json"

	var outpath strings.Builder

	fmt.Printf("parsing %s, output at %s\n", demo_path, outpath_arg)

	outpath.WriteString(outpath_arg)

	f, err := os.Open(demo_path)

	is_cs2 := is_demo_cs2(f)
	if is_cs2 {
		return
	}

	if err != nil {
		log.Panic("failed to open demo file: ", err)
	}
	defer f.Close()

	p := dem.NewParser(f)
	defer p.Close()

	match_start := false
	events_map := make(map[string][]interface{})

	// Update match_start, don't want to include warm up events
	p.RegisterEventHandler(func(e events.IsWarmupPeriodChanged) {
		if e.NewIsWarmupPeriod == false && e.OldIsWarmupPeriod == true {
			match_start = true
		}
	})

	game_round := 1

	// Register handler for kills include position,
	p.RegisterEventHandler(func(e events.Kill) {
		if match_start {
			wallbang := false
			if e.PenetratedObjects > 0 {
				wallbang = true
			}

			var killer_position, victim_position r3.Vector

			var killer_name, victim_name string

			if e.Victim != nil {
				victim_position = e.Victim.Position()
				victim_name = e.Victim.Name
			}

			if e.Killer != nil {
				killer_position = e.Killer.Position()
				killer_name = e.Killer.Name
			}

			kill_map := map[string]interface{}{
				"round":           game_round,
				"killer":          killer_name,
				"victim":          victim_name,
				"weapon":          e.Weapon.String(),
				"head_shot":       e.IsHeadshot,
				"through_smoke":   e.ThroughSmoke,
				"wallbang":        wallbang,
				"killer_position": killer_position,
				"victim_position": victim_position,
			}

			events_map["kills"] = append(events_map["kills"], kill_map)
		}
	})

	p.RegisterEventHandler(func(g events.GrenadeEventIf) {
		if match_start {
			var thrower_pos r3.Vector
			var thrower string
			var nade string

			if g.Base().Thrower != nil {
				thrower_pos = g.Base().Thrower.Position()
				thrower = g.Base().Thrower.Name
			}

			if g.Base().Grenade != nil {
				nade = g.Base().Grenade.String()
			}

			nade_map := map[string]interface{}{
				"round":               game_round,
				"grenade_type":        g.Base().GrenadeType.String(),
				"grenade":             nade,
				"grenade_position":    g.Base().Position,
				"grenade_thrower":     thrower,
				"grenade_thrower_pos": thrower_pos,
			}

			events_map["grenades"] = append(events_map["grenades"], nade_map)
		}
	})

	p.RegisterEventHandler(func(wf events.WeaponFire) {
		if match_start {
			var shooter_pos r3.Vector

			if wf.Shooter != nil {
				shooter_pos = wf.Shooter.Position()
			}

			shots_map := map[string]interface{}{
				"round":       game_round,
				"shooter":     wf.Shooter.Name,
				"shooter_pos": shooter_pos,
				"weapon":      wf.Weapon.String(),
			}

			events_map["shots_fired"] = append(events_map["shots_fired"], shots_map)
		}
	})

	p.RegisterEventHandler(func(ph events.PlayerHurt) {
		if match_start {
			var player_hurt_pos, attacker_pos r3.Vector
			var player_hurt, attacker string

			if ph.Player != nil {
				player_hurt_pos = ph.Player.Position()
				player_hurt = ph.Player.Name
			}

			if ph.Attacker != nil {
				attacker_pos = ph.Player.Position()
				attacker = ph.Attacker.Name
			}

			player_hurt_map := map[string]interface{}{
				"round":           game_round,
				"player_hurt_pos": player_hurt_pos,
				"attacker_pos":    attacker_pos,
				"player_hurt":     player_hurt,
				"attacker":        attacker,
				"health":          ph.Health,
				"armor":           ph.Armor,
				"health_damage":   ph.HealthDamage,
				"armor_damage":    ph.ArmorDamage,
				"hit_group":       ph.HitGroup,
				"weapon":          ph.Weapon.String(),
			}

			events_map["player_damaged"] = append(events_map["player_damage"], player_hurt_map)
		}
	})
	// Using equipment value freeze time end, mainly going to be used to tell if round is buy round or eco
	// Because of this don't need to know what was bought. Kills kind of covers how weapons are used.
	p.RegisterEventHandler(func(ge events.RoundFreezetimeEnd) {
		if match_start {

			var econ_map map[string]interface{}
			// should probably use this m_unFreezetimeEndEquipmentValue property can then get value / equipment when freeze time ends if can't find a suitable event
			for _, pl := range p.GameState().Participants().Playing() {

				var items []string

				for _, item := range pl.Weapons() {
					// fmt.Printf("%s has a %s in round %d\n", pl.Name, item.String(), game_round)
					items = append(items, item.String())
				}

				round_start_value_without_defaults := pl.EquipmentValueRoundStart() - 200
				non_armour_equipment_value := round_start_value_without_defaults - pl.MoneySpentThisRound()

				if pl.Armor() == 100 {
					non_armour_equipment_value -= 100
				}

				econ_map = map[string]interface{}{
					"round":                      game_round,
					"player":                     pl.Name,
					"money_spent":                pl.MoneySpentThisRound(),
					"non_armour_equipment_value": non_armour_equipment_value,
					"armour":                     pl.Armor(),
					"helmet":                     pl.HasHelmet(),
					"net_spend":                  round_start_value_without_defaults - pl.MoneySpentThisRound(),
					"inventory":                  items, // This won't capture anything left on the ground and then picked up afer freeze time
					"team_money_spent":           pl.TeamState.MoneySpentThisRound(),
					"team_total_equipment_value": pl.TeamState.MoneySpentThisRound() + pl.TeamState.RoundStartEquipmentValue(),
					"team":                       pl.TeamState.ClanName(),
					"team_game_id":               pl.TeamState.ID(),
				}

				events_map["economy"] = append(events_map["economy"], econ_map)
			}
		}
	})

	// This doesn't seem to be fully accurate despite notes in the docs about it being the way to monitor game round.
	// Could look into it but won't provide much value, the round should be used as a key across the different events recorded.
	p.RegisterEventHandler(func(score events.ScoreUpdated) {
		if match_start {
			game_round++
		}
	})

	err = p.ParseToEnd()
	if err != nil {
		log.Panic("failed to parse demo: ", err)
	}

	j, j_err := json.Marshal(events_map)
	if j_err != nil {
		fmt.Printf("Error: %s", j_err.Error())
	}

	f_err := os.WriteFile(outpath.String(), j, 0644)
	if f_err != nil {
		panic(f_err)
	}

}
