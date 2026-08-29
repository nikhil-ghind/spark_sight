import { useEffect, useState } from "react";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer } from "recharts";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

function useFetch(path) {
  const [data, setData] = useState([]);
  useEffect(() => {
    fetch(`${API}${path}`).then(r => r.json()).then(setData).catch(console.error);
  }, [path]);
  return data;
}

function HourlyCounts() {
  const data = useFetch("/api/hourly-counts?limit=48");
  return (
    <section>
      <h2>Hourly Event Counts (last 48 h)</h2>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="hour" tick={{ fontSize: 10 }} />
          <YAxis />
          <Tooltip />
          <Line type="monotone" dataKey="event_count" stroke="#6366f1" dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </section>
  );
}

function DailyRevenue() {
  const data = useFetch("/api/daily-revenue?days=30");
  return (
    <section>
      <h2>Daily Revenue (last 30 days)</h2>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" tick={{ fontSize: 10 }} />
          <YAxis />
          <Tooltip />
          <Bar dataKey="total_revenue" fill="#10b981" />
        </BarChart>
      </ResponsiveContainer>
    </section>
  );
}

function Retention() {
  const data = useFetch("/api/retention");
  return (
    <section>
      <h2>User Retention (day-N active users)</h2>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="day" />
          <YAxis />
          <Tooltip />
          <Line type="monotone" dataKey="active_users" stroke="#f59e0b" dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </section>
  );
}

export default function App() {
  return (
    <main style={{ padding: "1.5rem", fontFamily: "sans-serif" }}>
      <h1>spark_sight dashboard</h1>
      <HourlyCounts />
      <DailyRevenue />
      <Retention />
    </main>
  );
}
