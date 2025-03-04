"use client";
import { useEffect, useState } from "react";

type GamePick = {
    game_id: number;
    home_team: string;
    away_team: string;
    runline: string;
    pick: string;
    Away_SP_Name: string;
    Home_SP_Name: string;
    Away_Batter1_Name: string;
    Away_Batter2_Name: string;
    Away_Batter3_Name: string;
    Away_Batter4_Name: string;
    Away_Batter5_Name: string;
    Away_Batter6_Name: string;
    Away_Batter7_Name: string;
    Away_Batter8_Name: string;
    Away_Batter9_Name: string;
    Home_Batter1_Name: string;
    Home_Batter2_Name: string;
    Home_Batter3_Name: string;
    Home_Batter4_Name: string;
    Home_Batter5_Name: string;
    Home_Batter6_Name: string;
    Home_Batter7_Name: string;
    Home_Batter8_Name: string;
    Home_Batter9_Name: string;
};

export default function Home() {
    const [games, setGames] = useState<GamePick[]>([]);
    const [error, setError] = useState("");
    const [selectedDate, setSelectedDate] = useState(new Date().toISOString().split("T")[0]);
    const [selectedGame, setSelectedGame] = useState<GamePick | null>(null);
    const [lastUpdated, setLastUpdated] = useState<string>("");

    const fetchPicks = (date: string) => {
        fetch(`/api/picks?date=${date}`)
            .then((res) => res.json())
            .then((data) => {
                if (data.error) {
                    setError(data.error);
                    setGames([]);
                } else {
                    setError("");
                    setGames(data);
                }
            })
            .catch((err) => {
                console.error("Error fetching picks:", err);
                setError("Failed to load picks.");
                setGames([]);
            });
    };

    // Fetch the last_updated value from the public folder
    useEffect(() => {
        fetch('/last_updated.csv')
            .then((res) => res.text())
            .then((data) => {
                const lines = data.trim().split("\n");
                if (lines.length >= 2) {
                    setLastUpdated(lines[1].trim());
                }
            })
            .catch((err) => {
                console.error("Error fetching last updated:", err);
            });
    }, []);

    useEffect(() => {
        fetchPicks(selectedDate);
    }, [selectedDate]);

    return (
        <div style={styles.container}>
            {/* Top right container for last updated info and links */}
            <div style={styles.topRight}>
                <div style={styles.lastUpdated}>
                    Last updated: {lastUpdated}
                </div>
                <div style={styles.links}>
                    <a 
                        href="https://www.cbssports.com/mlb/scoreboard/" 
                        target="_blank" 
                        rel="noopener noreferrer" 
                        style={styles.link}
                    >
                        Today&apos;s Games at CBS Sports
                    </a>
                    <a 
                        href="https://github.com/nathank00/MLB-Analytics" 
                        target="_blank" 
                        rel="noopener noreferrer" 
                        style={styles.link}
                    >
                        View Code on GitHub
                    </a>
                </div>
            </div>
            
            <h1 style={styles.title}>MLB Picks</h1>
            
            <input
                type="date"
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                style={styles.datePicker}
            />

            {error ? (
                <p style={styles.error}>{error}</p>
            ) : games.length === 0 ? (
                <p style={styles.noPicks}>No picks available.</p>
            ) : (
                <div style={styles.grid}>
                    {games.map((game, index) => (
                        <div 
                            key={index} 
                            style={styles.card} 
                            onClick={() => setSelectedGame(game)}
                        >
                            <h3 style={styles.teams}>
                                <span style={styles.teamName}>{game.home_team}</span> 
                                <span style={styles.vs}> vs </span> 
                                <span style={styles.teamName}>{game.away_team}</span>
                            </h3>
                            <p style={styles.text}>
                                Runline: <span style={styles.highlight}>{game.runline}</span>
                            </p>
                            <div 
                                style={{ 
                                    ...styles.pickBox, 
                                    backgroundColor: game.pick === "Over" ? "#4CAF50" : "#FF5252" 
                                }}
                            >
                                {game.pick}
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Popup Modal for Game Details */}
            {selectedGame && (
                <div style={styles.modalOverlay} onClick={() => setSelectedGame(null)}>
                    <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
                        <h2 style={styles.modalTitle}>
                            {selectedGame.home_team} vs {selectedGame.away_team}
                        </h2>
                        <p style={styles.modalText}>
                            <strong>Starting Pitchers:</strong> 
                            <br /> {selectedGame.away_team}: {selectedGame.Away_SP_Name} 
                            <br /> {selectedGame.home_team}: {selectedGame.Home_SP_Name}
                        </p>
                        <div style={styles.lineups}>
                            <div>
                                <h3 style={styles.teamTitle}>{selectedGame.away_team}</h3>
                                {Array.from({ length: 9 }, (_, i) => (
                                    <p key={i} style={styles.modalText}>
                                        <strong>{i + 1}.</strong> {selectedGame[`Away_Batter${i + 1}_Name` as keyof GamePick]}
                                    </p>
                                ))}
                            </div>
                            <div>
                                <h3 style={styles.teamTitle}>{selectedGame.home_team}</h3>
                                {Array.from({ length: 9 }, (_, i) => (
                                    <p key={i} style={styles.modalText}>
                                        <strong>{i + 1}.</strong> {selectedGame[`Home_Batter${i + 1}_Name` as keyof GamePick]}
                                    </p>
                                ))}
                            </div>
                        </div>
                        <button style={styles.closeButton} onClick={() => setSelectedGame(null)}>
                            Close
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}

// Styles
const styles: { [key: string]: React.CSSProperties } = {
    container: {
        backgroundColor: "#121212",
        color: "#1E90FF",
        minHeight: "100vh",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        padding: "20px",
    },
    topRight: {
        position: "fixed",
        top: "10px",
        right: "10px",
        textAlign: "right",
    },
    lastUpdated: {
        fontSize: "0.9rem",
        color: "#FFFFFF",
        backgroundColor: "#1E1E1E",
        padding: "5px 10px",
        borderRadius: "5px",
        marginBottom: "5px",
    },
    links: {
        display: "flex",
        flexDirection: "column",
        alignItems: "flex-end",
    },
    link: {
        color: "#1E90FF",
        textDecoration: "none",
        fontSize: "0.9rem",
        marginTop: "5px",
    },
    title: {
        fontSize: "2.5rem",
        fontWeight: "bold",
        marginBottom: "20px",
    },
    datePicker: {
        backgroundColor: "#1E1E1E",
        border: "1px solid #1E90FF",
        padding: "10px",
        fontSize: "16px",
        borderRadius: "5px",
        color: "#1E90FF",
        outline: "none",
        marginBottom: "20px",
    },
    grid: {
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))",
        gap: "20px",
        width: "80%",
        maxWidth: "900px",
    },
    card: {
        backgroundColor: "#1E1E1E",
        padding: "20px",
        borderRadius: "10px",
        boxShadow: "0 4px 10px rgba(0, 0, 0, 0.3)",
        textAlign: "center",
        border: "2px solid #1E90FF",
        cursor: "pointer",
        position: "relative",
    },
    teams: {
        fontSize: "2rem",
        fontWeight: "bold",
    },
    teamName: {
        color: "#1E90FF",
        fontWeight: "bold",
    },
    vs: {
        color: "#1E90FF",
        fontSize: "1.5rem",
    },
    text: {
        fontSize: "1.2rem",
        marginTop: "10px",
    },
    pickBox: {
        fontSize: "2rem",
        fontWeight: "bold",
        color: "#FFFFFF",
        padding: "15px 25px",
        borderRadius: "10px",
        display: "inline-block",
        marginTop: "15px",
        textTransform: "uppercase",
    },
    modalOverlay: {
        position: "fixed",
        top: 0,
        left: 0,
        width: "100vw",
        height: "100vh",
        backgroundColor: "rgba(0, 0, 0, 0.7)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
    },
    modal: {
        backgroundColor: "#1E1E1E",
        padding: "20px",
        borderRadius: "10px",
        border: "2px solid #1E90FF",
        maxWidth: "600px",
        textAlign: "center",
    },
    modalTitle: { fontSize: "1.8rem", marginBottom: "10px" },
    modalText: { fontSize: "1.2rem", margin: "5px 0" },
    teamTitle: { fontSize: "1.5rem", color: "#1E90FF" },
    lineups: { display: "flex", justifyContent: "space-around", marginTop: "15px" },
    closeButton: { marginTop: "20px", padding: "10px 20px", cursor: "pointer", border: "none", backgroundColor: "#1E90FF", color: "#121212" },
    error: {
        color: "#FF5252",
        marginTop: "20px",
    },
    noPicks: {
        marginTop: "20px",
    },
};
