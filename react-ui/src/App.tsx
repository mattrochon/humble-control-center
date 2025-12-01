import { Routes, Route, Navigate } from "react-router-dom";
import Layout from "./components/Layout";
import Home from "./pages/Home";
import Library from "./pages/Library";
import ItemPage from "./pages/ItemPage";
import Admin from "./pages/Admin";
import Purchases from "./pages/Purchases";
import Bundle from "./pages/Bundle";
import Settings from "./pages/Settings";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Home />} />
        <Route path="/library" element={<Library />} />
        <Route path="/item/:id" element={<ItemPage />} />
        <Route path="/admin" element={<Admin />} />
        <Route path="/purchases" element={<Purchases />} />
        <Route path="/bundle/:orderId" element={<Bundle />} />
        <Route path="/settings" element={<Settings />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
